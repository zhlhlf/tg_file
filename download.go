package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/amarnathcjd/gogram/telegram"
)

var fileNameSanitizer = regexp.MustCompile(`[<>:"/\\|?*\x00-\x1F]`)

// 从 Telegram 频道 URL 中提取频道用户名
// 支持格式: https://t.me/channelname 或 t.me/channelname
func extractChannelNameFromURL(url string) string {
	url = strings.TrimSpace(url)

	// 移除 https:// 或 http://
	if idx := strings.Index(url, "://"); idx != -1 {
		url = url[idx+3:]
	}

	// 提取 t.me/ 之后的部分
	if strings.Contains(url, "t.me/") {
		url = strings.SplitN(url, "t.me/", 2)[1]
	}

	// 移除查询参数和锚点
	if idx := strings.IndexAny(url, "?#"); idx != -1 {
		url = url[:idx]
	}

	// 移除尾部斜杠
	url = strings.TrimSuffix(url, "/")

	if url != "" && !strings.Contains(url, "/") {
		return url
	}
	return ""
}

func normalizeTelegramPeerTarget(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if strings.HasPrefix(raw, "@") {
		return raw
	}
	name := extractChannelNameFromURL(raw)
	if name != "" {
		return "@" + name
	}
	// 邀请链接 / + 链接不能转成 @username，直接保留原值交给底层解析
	if strings.Contains(raw, "t.me/+") || strings.Contains(raw, "joinchat/") {
		return raw
	}
	return raw
}

func (infos *Infos) startConfiguredDownloads(ctx context.Context) {
	if !infos.DownloadStarted.CompareAndSwap(false, true) {
		return
	}

	if infos.UserClient == nil && len(infos.UserClients) == 0 {
		log.Printf("自动下载未启动: 无可用 UserBot 客户端")
		return
	}

	if !infos.Conf.Download.Enabled || len(infos.Conf.Download.Channels) == 0 {
		log.Printf("自动下载未启用或未配置频道")
		return
	}

	outputRoot := strings.TrimSpace(infos.Conf.Download.OutputDir)
	if outputRoot == "" {
		outputRoot = "downloads"
	}
	if !filepath.IsAbs(outputRoot) {
		// Use current working directory (binary run root) as base for relative outputDir
		if cwd, err := os.Getwd(); err == nil {
			outputRoot = filepath.Join(cwd, outputRoot)
		} else {
			// fallback to files path if Getwd fails
			outputRoot = filepath.Join(infos.FilesPath, outputRoot)
		}
	}
	if err := os.MkdirAll(outputRoot, 0755); err != nil {
		log.Printf("创建下载目录失败: %v", err)
		return
	}

	if len(infos.BotClients) > 0 {
		if err := infos.prepareRelayBots(); err != nil {
			log.Printf("下载 Bot 不可用，将回退为 UserBot 下载: %v", err)
		}
	} else {
		log.Printf("未配置 BotToken，使用 UserBot 下载")
	}

	log.Printf("自动下载开始执行, 频道数: %d, 输出目录: %s", len(infos.Conf.Download.Channels), outputRoot)
	if len(infos.RelayBotClients) > 0 {
		log.Printf("已启用 Bot 分流下载: 可用Bot: %d", len(infos.RelayBotClients))
	}
	infos.logDownloadMemberships(ctx)
	// 并发控制: `concurrent` 限制同时进行的文件下载数量（不是频道）
	concurrency := infos.Conf.Download.Concurrent
	if concurrency <= 0 {
		concurrency = 3
	}
	sem := make(chan struct{}, concurrency)
	var wgFiles sync.WaitGroup
	availableAccounts := infos.availableUserAccounts()
	rrIdx := 0
	log.Printf("userbot可用账号列表(%d): %v", len(availableAccounts), availableAccounts)

	// 自动解析缺少 ID 的频道 URL
	for i, task := range infos.Conf.Download.Channels {
		log.Printf("频道[%d]: ID: %d, Join: %s", i, task.ID, task.Join)
		if task.ID == 0 && task.Join != "" {
			channelName := extractChannelNameFromURL(task.Join)
			if channelName != "" {
				// 使用第一个可用的 UserClient 解析频道 ID
				var resolvedPeer interface{}
				var err error
				client := infos.UserClient
				if client == nil && len(availableAccounts) > 0 {
					client = infos.UserClients[availableAccounts[0]]
				}

				if client != nil {
					resolvedPeer, err = client.ResolvePeer(fmt.Sprintf("@%s", channelName))
					if err != nil {
						log.Printf("错误: 解析频道 @%s 失败: %v", channelName, err)
					} else {
						log.Printf("成功解析: %#v", resolvedPeer)
					}
				} else {
					log.Printf("错误: 没有可用的 UserClient")
				}

				if err == nil && resolvedPeer != nil {
					// 使用反射获取 ChannelID 字段
					rValue := reflect.ValueOf(resolvedPeer)
					if rValue.Kind() == reflect.Ptr {
						rValue = rValue.Elem()
					}

					// 尝试获取 ChannelID 字段（InputPeerChannel）或 ID 字段（其他类型）
					var channelID int64
					idField := rValue.FieldByName("ChannelID")
					if !idField.IsValid() {
						idField = rValue.FieldByName("ID")
					}

					if idField.IsValid() && idField.CanInt() {
						channelID = idField.Int()
						infos.Conf.Download.Channels[i].ID = channelID
						// 检查所有 UserBot 是否已加入该频道（避免依赖本地 numeric ID 缓存，优先按 join 名称 ResolvePeer）
						accountNames := make([]string, 0, len(infos.UserClients))
						for name := range infos.UserClients {
							accountNames = append(accountNames, name)
						}
						sort.Strings(accountNames)
						channelNameForResolve := ""
						if task.Join != "" {
							channelNameForResolve = extractChannelNameFromURL(task.Join)
						}
						for _, an := range accountNames {
							c := infos.UserClients[an]
							if c == nil {
								log.Printf("账号 %s 未就绪，跳过加入检查", an)
								continue
							}
							peerCID := channelID
							// 优先使用 join 名称在该账号上 ResolvePeer，避免依赖本地 numeric ID 缓存
							if channelNameForResolve != "" {
								resolved, rerr := c.ResolvePeer(fmt.Sprintf("@%s", channelNameForResolve))
								if rerr != nil {
									log.Printf("账号 %s ResolvePeer @%s 失败: %v", an, channelNameForResolve, rerr)
									// 在允许强制加入时尝试加入，然后重试 ResolvePeer
									if infos.Conf.Download.ForceJoin || task.ForceJoin {
										if jerr := tryJoinChannel(c, task.Join); jerr != nil {
											log.Printf("账号 %s 尝试加入频道失败: %v", an, jerr)
											continue
										}
										resolved, rerr = c.ResolvePeer(fmt.Sprintf("@%s", channelNameForResolve))
										if rerr != nil {
											log.Printf("账号 %s 加入后 ResolvePeer 仍失败: %v", an, rerr)
											continue
										}
									}
								}
								// 从 resolved 中提取实际的 peer id
								if resolved != nil {
									rv := reflect.ValueOf(resolved)
									if rv.Kind() == reflect.Ptr {
										rv = rv.Elem()
									}
									idf := rv.FieldByName("ChannelID")
									if !idf.IsValid() {
										idf = rv.FieldByName("ID")
									}
									if idf.IsValid() && idf.CanInt() {
										peerCID = idf.Int()
									} else {
										log.Printf("账号 %s 无法从 ResolvePeer 返回值中提取 ID: %T", an, resolved)
										continue
									}
								}
							}
							// 使用解析到的 peerCID 检查该账号是否能访问频道
							if _, err := infos.getLatestMessageID(c, peerCID); err == nil {
								log.Printf("账号 %s 已加入频道 cid: %d", an, peerCID)
							} else {
								log.Printf("账号 %s 未加入频道 cid: %d: %v", an, peerCID, err)
								// 若未加入且允许强制加入，则尝试加入
								if infos.Conf.Download.ForceJoin || task.ForceJoin {
									if jerr := tryJoinChannel(c, task.Join); jerr == nil {
										if _, err2 := infos.getLatestMessageID(c, peerCID); err2 == nil {
											log.Printf("账号 %s 成功加入频道 cid: %d", an, peerCID)
										} else {
											log.Printf("账号 %s 加入后仍不可用 cid: %d: %v", an, peerCID, err2)
										}
									} else {
										log.Printf("账号 %s 尝试加入频道失败: %v", an, jerr)
									}
								}
							}
						}
					} else {
						log.Printf("警告: 无法从返回值中获取 ChannelID/ID 字段, 返回类型: %T", resolvedPeer)
					}
				} else {
					log.Printf("警告: 无法解析频道 %s 的 ID: %v", channelName, err)
				}
			}
		}
	}

	for _, task := range infos.Conf.Download.Channels {
		if task.ID == 0 {
			continue
		}

		candidateAccounts := infos.channelCandidateAccounts(task, availableAccounts, &rrIdx)

		if len(candidateAccounts) == 0 {
			log.Printf("频道下载跳过: cid: %d user: %s, 未找到可用 UserBot", task.ID, task.User)
			continue
		}

		var lastErr error
		for idx, accountName := range candidateAccounts {
			client := infos.resolveTaskClient(task, accountName)
			if client == nil {
				if client == nil {
					lastErr = fmt.Errorf("未找到可用 UserBot")
					log.Printf("频道下载失败: cid: %d user: %s err: %v", task.ID, accountName, lastErr)
					continue
				}
			}

			if err := infos.downloadChannelRange(ctx, client, outputRoot, task, sem, &wgFiles, accountName); err != nil {
				lastErr = err
				log.Printf("频道下载失败: cid: %d user: %s err: %v", task.ID, accountName, err)
				if strings.TrimSpace(task.User) == "" && len(infos.RelayBotClients) > 0 && idx < len(candidateAccounts)-1 {
					log.Printf("频道下载切换下一个账号: cid: %d from: %s to: %s", task.ID, accountName, candidateAccounts[idx+1])
					continue
				}
			} else {
				lastErr = nil
				break
			}
		}
		if lastErr != nil {
			continue
		}
	}
	wgFiles.Wait()
	log.Printf("自动下载任务执行完成")

	// 初始化 LastDownloaded map（记录每个频道已下载到的最新消息ID）
	if infos.LastDownloaded == nil {
		infos.LastDownloaded = make(map[int64]int32)
	}
	for _, task := range infos.Conf.Download.Channels {
		if task.ID == 0 {
			continue
		}
		// 使用 clientForTask 获取可用客户端以查询 latest
		client := infos.clientForTask(task)
		if client == nil {
			continue
		}
		if latest, err := infos.getLatestMessageID(client, task.ID); err == nil {
			infos.Mutex.Lock()
			infos.LastDownloaded[task.ID] = latest
			infos.Mutex.Unlock()
		}
	}

	// 若配置了扫描间隔（或未配置），则启动周期性增量检查。
	// 行为：初次全量下载完成后进入循环 -> 每次完成一次检测与（如有）增量下载后，等待 scanInterval 再次检测。
	if infos.Conf != nil {
		scanSec := infos.Conf.Download.ScanInterval
		if scanSec <= 0 {
			// 默认 5 分钟
			scanSec = 300
		}
		scanInterval := time.Duration(scanSec) * time.Second
		go func() {
			for {
				// 在每次循环开始前检查上下文是否已取消
				select {
				case <-ctx.Done():
					return
				default:
				}

				// 扫描开始（单线程定时，不再使用 ScanRunning 原子标志）
				func() {
					for _, task := range infos.Conf.Download.Channels {
						if task.ID == 0 {
							continue
						}
						client := infos.clientForTask(task)
						if client == nil {
							continue
						}
						latest, err := infos.getLatestMessageID(client, task.ID)
						if err != nil {
							continue
						}
						infos.Mutex.Lock()
						last := infos.LastDownloaded[task.ID]
						infos.Mutex.Unlock()
						if latest > last {
							from := last + 1
							log.Printf("发现频道新消息: cid: %d last: %d latest: %d, 开始增量下载", task.ID, last, latest)
							// 为本次增量下载创建独立的并发控制与等待组
							concurrency := infos.Conf.Download.Concurrent
							if concurrency <= 0 {
								concurrency = 3
							}
							sem := make(chan struct{}, concurrency)
							var wg sync.WaitGroup
							// 复制 task 并设置起始位置
							t := task
							t.FromMessageID = from
							// 使用 clientNameForTask 确定用于下载的账号
							acctName, _ := infos.clientNameForTask(task)
							if err := infos.downloadChannelRange(context.Background(), client, outputRoot, t, sem, &wg, acctName); err == nil {
								wg.Wait()
								infos.Mutex.Lock()
								infos.LastDownloaded[task.ID] = latest
								infos.Mutex.Unlock()
							} else {
								log.Printf("增量下载失败: cid: %d err: %v", task.ID, err)
							}
						}
					}
				}()

				// 本次扫描与可能的增量下载完成后，等待完整的扫描间隔再进行下一次检测
				time.Sleep(scanInterval)
			}
		}()
	}
}

func (infos *Infos) clientForTask(task DownloadChannel) *telegram.Client {
	user := strings.TrimSpace(task.User)
	if user != "" {
		if client, ok := infos.UserClients[user]; ok {
			return client
		}
		return nil
	}
	if infos.DefaultUserName != "" {
		if client, ok := infos.UserClients[infos.DefaultUserName]; ok {
			return client
		}
	}
	if infos.UserClient != nil {
		return infos.UserClient
	}
	for _, client := range infos.UserClients {
		if client != nil {
			return client
		}
	}
	return nil
}

func (infos *Infos) downloadChannelRange(ctx context.Context, client *telegram.Client, outputRoot string, task DownloadChannel, sem chan struct{}, wgFiles *sync.WaitGroup, accountName string) error {
	latest, err := infos.getLatestMessageID(client, task.ID)
	if err != nil {
		// 如果配置允许强制加入频道，则尝试加入并重试一次
		forceJoin := infos.Conf.Download.ForceJoin || task.ForceJoin
		if forceJoin {
			log.Printf("账号 %s 未加入频道 cid: %d, 尝试强制加入", accountName, task.ID)
			if jerr := tryJoinChannel(client, task.Join); jerr == nil {
				latest, err = infos.getLatestMessageID(client, task.ID)
			} else {
				log.Printf("尝试加入频道失败: cid: %d join: %s err: %v", task.ID, task.Join, jerr)
				return err
			}
		} else {
			return err
		}
	}
	if latest == 0 {
		return nil
	}

	start := task.FromMessageID
	if start <= 0 {
		start = 1
	}
	if start > latest {
		log.Printf("频道无需下载: cid: %d from: %d latest: %d", task.ID, start, latest)
		return nil
	}

	typeFilter, allowAll := normalizeTypeFilter(infos.Conf.Download.GlobalTypes, task.Types)
	fetchMode := resolveMessageFetchMode(infos, typeFilter, allowAll)
	searchFilters := buildMediaSearchFilters(typeFilter, allowAll)
	log.Printf("频道开始下载: cid: %d from: %d latest: %d user: %s fetch: %s", task.ID, start, latest, accountName, fetchMode)

	availableAccounts := infos.availableUserAccounts()
	rrIdx := 0
	var relayIdx uint64
	// batchSize 可从配置覆盖，默认 100
	bs := 100
	if infos != nil && infos.Conf != nil && infos.Conf.Download.BatchSize > 0 {
		bs = infos.Conf.Download.BatchSize
	}
	if bs > 100 {
		// Telegram search/history 单次实用上限按 100 处理
		bs = 100
	}
	batchSize := int32(bs)
	workerCount := cap(sem)
	if workerCount <= 0 {
		workerCount = 1
	}
	batchDelaySec := 4
	if infos != nil && infos.Conf != nil && infos.Conf.Download.BatchDelay > 0 {
		batchDelaySec = infos.Conf.Download.BatchDelay
	}

	type downloadJob struct {
		msg     telegram.NewMessage
		client  *telegram.Client
		account string
		cache   *mediaResolveCache
		task    DownloadChannel
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	jobs := make(chan downloadJob, bs)
	const refillThreshold int32 = 30
	var queuedJobs atomic.Int32
	var exhausted atomic.Bool
	var closeJobsOnce sync.Once
	var producerWG sync.WaitGroup
	var workerWG sync.WaitGroup

	// 拉取状态：search/history 用 offset 翻页；ids 用连续游标兜底
	idsCursor := start
	offsetID := int32(0)
	filterIdx := 0
	seenMsgIDs := make(map[int32]struct{}, 256)
	minIDBound := start - 1
	if start <= 1 {
		minIDBound = 0
	}
	maxIDBound := latest + 1
	if latest >= 2147483647 {
		maxIDBound = latest
	}

	closeJobs := func() {
		closeJobsOnce.Do(func() {
			close(jobs)
		})
	}

	markExhausted := func() {
		exhausted.Store(true)
		if queuedJobs.Load() == 0 {
			closeJobs()
		}
	}

	advanceSearchFilter := func() bool {
		filterIdx++
		offsetID = 0
		if filterIdx >= len(searchFilters) {
			return false
		}
		debugf("切换媒体搜索过滤器: cid: %d filterIndex: %d/%d", task.ID, filterIdx+1, len(searchFilters))
		return true
	}

	go func() {
		<-ctx.Done()
		closeJobs()
	}()

	fetchNextJobs := func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			var (
				ms        []telegram.NewMessage
				err       error
				pageDesc  string
				rawCount  int
				oldestID  int32
				hasOldest bool
			)

			switch fetchMode {
			case "ids":
				if idsCursor > latest {
					markExhausted()
					return nil
				}
				cursor := idsCursor
				end := cursor + batchSize - 1
				if end > latest {
					end = latest
				}
				idsCursor = end + 1
				ids := make([]int32, 0, end-cursor+1)
				for mid := cursor; mid <= end; mid++ {
					ids = append(ids, mid)
				}
				pageDesc = fmt.Sprintf("ids[%d-%d]", cursor, end)
				log.Printf("开始批量拉取消息: cid: %d mode: ids range: %d-%d queued: %d", task.ID, cursor, end, queuedJobs.Load())
				ms, err = client.GetMessages(task.ID, &telegram.SearchOption{IDs: ids})
				if err != nil {
					log.Printf("批量获取消息失败: cid: %d mode: ids range: %d-%d err: %v", task.ID, cursor, end, err)
					continue
				}

			case "history":
				pageDesc = fmt.Sprintf("history(offset=%d min=%d max=%d)", offsetID, minIDBound, maxIDBound)
				log.Printf("开始批量拉取消息: cid: %d mode: history offset: %d minID: %d maxID: %d queued: %d", task.ID, offsetID, minIDBound, maxIDBound, queuedJobs.Load())
				ms, err = client.GetHistory(task.ID, &telegram.HistoryOption{
					Limit:  batchSize,
					Offset: offsetID,
					MinID:  minIDBound,
					MaxID:  maxIDBound,
				})
				if err != nil {
					log.Printf("批量获取消息失败: cid: %d mode: history offset: %d err: %v", task.ID, offsetID, err)
					log.Printf("历史拉取失败，回退到 ids 模式: cid: %d", task.ID)
					fetchMode = "ids"
					idsCursor = start
					offsetID = 0
					continue
				}

			default: // search
				if len(searchFilters) == 0 {
					log.Printf("search 模式无可用过滤器，切换到 history: cid: %d", task.ID)
					fetchMode = "history"
					offsetID = 0
					continue
				}
				if filterIdx >= len(searchFilters) {
					markExhausted()
					return nil
				}
				filter := searchFilters[filterIdx]
				pageDesc = fmt.Sprintf("search(filter=%s offset=%d min=%d max=%d)", mediaFilterName(filter), offsetID, minIDBound, maxIDBound)
				log.Printf("开始批量拉取消息: cid: %d mode: search filter: %s offset: %d minID: %d maxID: %d queued: %d", task.ID, mediaFilterName(filter), offsetID, minIDBound, maxIDBound, queuedJobs.Load())
				ms, err = client.GetMessages(task.ID, &telegram.SearchOption{
					Query:  "",
					Filter: filter,
					Offset: offsetID,
					Limit:  batchSize,
					MinID:  minIDBound,
					MaxID:  maxIDBound,
				})
				if err != nil {
					log.Printf("批量获取消息失败: cid: %d mode: search filter: %s offset: %d err: %v", task.ID, mediaFilterName(filter), offsetID, err)
					if advanceSearchFilter() {
						continue
					}
					log.Printf("媒体搜索失败，回退到 history 模式: cid: %d", task.ID)
					fetchMode = "history"
					offsetID = 0
					continue
				}
			}

			rawCount = len(ms)
			for _, msg := range ms {
				if !hasOldest || msg.ID < oldestID {
					oldestID = msg.ID
					hasOldest = true
				}
			}

			// 更新翻页游标（search/history 都是 offset_id 语义：继续取更旧消息）
			if fetchMode != "ids" {
				if rawCount == 0 || !hasOldest {
					if fetchMode == "search" {
						if advanceSearchFilter() {
							continue
						}
					}
					markExhausted()
					return nil
				}
				if offsetID > 0 && oldestID >= offsetID {
					debugf("拉取游标无进展，结束当前过滤器: cid: %d mode: %s offset: %d oldest: %d", task.ID, fetchMode, offsetID, oldestID)
					if fetchMode == "search" {
						if advanceSearchFilter() {
							continue
						}
					}
					markExhausted()
					return nil
				}
				offsetID = oldestID
			}

			if rawCount == 0 {
				continue
			}

			messageCache := newMediaResolveCache(ms)
			sort.Slice(ms, func(i, j int) bool { return ms[i].ID < ms[j].ID })

			enqueued := 0
			for _, msg := range ms {
				if msg.ID < start || msg.ID > latest {
					continue
				}
				if _, seen := seenMsgIDs[msg.ID]; seen {
					continue
				}
				if msg.File == nil || !msg.IsMedia() {
					continue
				}

				msgType, ok := detectMessageType(msg)
				if !ok {
					continue
				}
				if !allowAll {
					if _, exists := typeFilter[msgType]; !exists {
						continue
					}
				}
				if infos.shouldSkipByFileSize(task, msg) {
					continue
				}

				seenMsgIDs[msg.ID] = struct{}{}
				fileAccount, fileClient := infos.selectFileDownloadClient(task, accountName, client, availableAccounts, &rrIdx)
				wgFiles.Add(1)
				queuedJobs.Add(1)

				job := downloadJob{msg: msg, client: fileClient, account: fileAccount, cache: messageCache, task: task}
				select {
				case <-ctx.Done():
					queuedJobs.Add(-1)
					wgFiles.Done()
					return ctx.Err()
				case jobs <- job:
					enqueued++
				}
			}

			log.Printf("拉取分页完成: cid: %d page: %s raw: %d enqueued: %d queued: %d", task.ID, pageDesc, rawCount, enqueued, queuedJobs.Load())

			// search/history 到达下界后，结束当前 filter
			if fetchMode != "ids" && hasOldest && oldestID <= start {
				if fetchMode == "search" {
					if enqueued > 0 {
						if !advanceSearchFilter() {
							exhausted.Store(true)
							if queuedJobs.Load() == 0 {
								closeJobs()
							}
						}
						return nil
					}
					if advanceSearchFilter() {
						continue
					}
					markExhausted()
					return nil
				}
				if enqueued > 0 {
					exhausted.Store(true)
					if queuedJobs.Load() == 0 {
						closeJobs()
					}
					return nil
				}
				markExhausted()
				return nil
			}

			if enqueued > 0 {
				return nil
			}
			// 本页没有可下载媒体，继续拉下一页
		}
	}

	producerWG.Add(1)
	go func() {
		defer producerWG.Done()

		for {
			if batchDelaySec > 0 {
				debugf("下载队列生产者休眠: cid: %d queued: %d sleep: %ds", task.ID, queuedJobs.Load(), batchDelaySec)
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Duration(batchDelaySec) * time.Second):
				}
			}

			if exhausted.Load() {
				if queuedJobs.Load() == 0 {
					closeJobs()
					return
				}
			} else {
				queued := queuedJobs.Load()
				if queued < refillThreshold {
					debugf("下载队列低于阈值，准备补充: cid: %d queued: %d threshold: %d", task.ID, queued, refillThreshold)
					if err := fetchNextJobs(); err != nil && ctx.Err() == nil {
						log.Printf("补充下载队列失败: cid: %d queued: %d err: %v", task.ID, queuedJobs.Load(), err)
					}
				}
			}

			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()

	workerWG.Add(workerCount)
	for i := 0; i < workerCount; i++ {
		go func() {
			defer workerWG.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case job, ok := <-jobs:
					if !ok {
						return
					}

					queuedJobs.Add(-1)

					if job.client == nil {
						log.Printf("下载消息失败: cid: %d mid: %d user: %s err: %v", task.ID, job.msg.ID, job.account, fmt.Errorf("未找到可用客户端"))
						wgFiles.Done()
						if exhausted.Load() && queuedJobs.Load() == 0 {
							closeJobs()
						}
						continue
					}

					var jobErr error
					var result *downloadResult
					const maxAttempts = 3
					for attempt := 1; attempt <= maxAttempts; attempt++ {
						result, jobErr = infos.downloadMessage(ctx, job.client, job.client, outputRoot, job.msg, job.msg, job.account, &relayIdx, job.cache, job.task)
						if jobErr == nil {
							break
						}
						if attempt < maxAttempts {
							debugf("下载消息失败，准备重试: cid: %d mid: %d user: %s attempt: %d/%d err: %v", task.ID, job.msg.ID, job.account, attempt, maxAttempts, jobErr)
							time.Sleep(time.Duration(attempt) * 2 * time.Second)
						}
					}
					if jobErr != nil {
						errorf("下载消息失败: cid: %d mid: %d user: %s err: %v", task.ID, job.msg.ID, job.account, jobErr)
					} else if result != nil && !result.Handled && infos != nil && infos.Conf != nil && infos.Conf.Download.Rclone.Enabled {
						remotePath, remoteErr := infos.rcloneRemotePath(outputRoot, result.FinalPath)
						if remoteErr != nil {
							errorf("rclone 路径计算失败: cid: %d mid: %d user: %s path: %s err: %v", task.ID, job.msg.ID, job.account, result.FinalPath, remoteErr)
						} else {
							mode := infos.rcloneTransferMode()
							if transferErr := infos.rcloneTransferFile(ctx, result.FinalPath, remotePath, mode); transferErr != nil {
								errorf("rclone %s 失败(已忽略): cid: %d mid: %d user: %s local: %s remote: %s err: %v", mode, task.ID, job.msg.ID, job.account, result.FinalPath, remotePath, transferErr)
							} else {
								log.Printf("rclone %s 完成: %s", mode, result.FinalPath)
							}
						}
					}
					wgFiles.Done()

					if exhausted.Load() && queuedJobs.Load() == 0 {
						closeJobs()
					}
				}
			}
		}()
	}

	workerWG.Wait()
	producerWG.Wait()
	return ctx.Err()
}

func (infos *Infos) availableUserAccounts() []string {
	accountNames := make([]string, 0, len(infos.UserClients))
	for name := range infos.UserClients {
		accountNames = append(accountNames, name)
	}
	sort.Strings(accountNames)
	availableAccounts := make([]string, 0, len(accountNames))
	for _, name := range accountNames {
		if infos.UserClients[name] != nil {
			availableAccounts = append(availableAccounts, name)
		}
	}
	return availableAccounts
}

func (infos *Infos) channelCandidateAccounts(task DownloadChannel, availableAccounts []string, rrIdx *int) []string {
	if user := strings.TrimSpace(task.User); user != "" {
		return []string{user}
	}
	if len(infos.RelayBotClients) > 0 && len(availableAccounts) > 0 {
		return append([]string(nil), availableAccounts...)
	}
	if len(availableAccounts) > 0 {
		assignedIdx := *rrIdx % len(availableAccounts)
		*rrIdx++
		return []string{availableAccounts[assignedIdx]}
	}
	if accountName, client := infos.clientNameForTask(task); client != nil {
		return []string{accountName}
	}
	return nil
}

func (infos *Infos) resolveTaskClient(task DownloadChannel, accountName string) *telegram.Client {
	if client := infos.UserClients[accountName]; client != nil {
		return client
	}
	if strings.TrimSpace(task.User) != "" {
		_, client := infos.clientNameForTask(task)
		return client
	}
	return nil
}

func (infos *Infos) selectFileDownloadClient(task DownloadChannel, accountName string, client *telegram.Client, availableAccounts []string, rrIdx *int) (string, *telegram.Client) {
	if len(infos.RelayBotClients) > 0 && strings.TrimSpace(task.User) == "" {
		return accountName, client
	}
	if strings.TrimSpace(task.User) != "" {
		return infos.clientNameForTask(task)
	}
	if len(availableAccounts) > 0 {
		selected := availableAccounts[*rrIdx%len(availableAccounts)]
		*rrIdx++
		return selected, infos.UserClients[selected]
	}
	return infos.clientNameForTask(task)
}

func (infos *Infos) shouldSkipByFileName(fileName, skipPath string, task DownloadChannel) bool {
	if infos == nil || infos.Conf == nil {
		return false
	}
	fileNameLower := strings.ToLower(fileName)
	requireNameContains := infos.Conf.Download.RequireNameContains
	if len(task.RequireNameContains) > 0 {
		requireNameContains = task.RequireNameContains
	}
	if len(requireNameContains) > 0 {
		matched := false
		for _, rawKeyword := range requireNameContains {
			keyword := strings.TrimSpace(rawKeyword)
			if keyword == "" {
				continue
			}
			if strings.Contains(fileNameLower, strings.ToLower(keyword)) {
				matched = true
				break
			}
		}
		if !matched {
			log.Printf("未命中必含过滤规则 跳过: path: %s", skipPath)
			return true
		}
	}
	for _, rawKeyword := range infos.Conf.Download.SkipNameContains {
		keyword := strings.TrimSpace(rawKeyword)
		if keyword == "" {
			continue
		}
		if strings.Contains(fileNameLower, strings.ToLower(keyword)) {
			log.Printf("命中过滤规则 跳过: filter: %q path: %s", keyword, skipPath)
			return true
		}
	}
	return false
}

// 复用“已存在”检测：同时检查本地文件与远端 rclone 是否存在。
// 返回值: localExists, remoteExists, remoteMatchMode, err(仅 rclone 检查错误)
func (infos *Infos) shouldSkipByFileSize(task DownloadChannel, msg telegram.NewMessage) bool {
	if infos == nil || infos.Conf == nil || msg.File == nil {
		return false
	}
	maxSize := infos.Conf.Download.MaxSize
	if task.MaxSize > 0 {
		maxSize = task.MaxSize
	}
	if maxSize <= 0 || msg.File.Size <= 0 || msg.File.Size <= maxSize {
		return false
	}
	log.Printf("命中文件大小过滤规则 跳过: cid: %d mid: %d size: %s maxSize: %s", task.ID, msg.ID, formatByteSize(msg.File.Size), formatByteSize(maxSize))
	return true
}

func formatByteSize(size int64) string {
	if size < 1024 {
		return fmt.Sprintf("%dB", size)
	}
	value := float64(size)
	units := []string{"B", "KB", "MB", "GB", "TB"}
	idx := 0
	for value >= 1024 && idx < len(units)-1 {
		value /= 1024
		idx++
	}
	return fmt.Sprintf("%.2f%s", value, units[idx])
}

func (infos *Infos) checkExistingLocalOrRemote(ctx context.Context, outputRoot, finalPath string) (bool, bool, string, error) {
	localExists := false
	if _, statErr := os.Stat(finalPath); statErr == nil {
		localExists = true
		// 本地已存在时直接返回，避免额外的远端检查开销
		return true, false, "", nil
	}

	remoteExists := false
	remoteMatchMode := ""
	if infos != nil && infos.Conf != nil && infos.Conf.Download.Rclone.Enabled {
		exists, matchMode, err := infos.rcloneFileExists(ctx, outputRoot, finalPath)
		if err != nil {
			return localExists, false, "", err
		}
		remoteExists = exists
		remoteMatchMode = matchMode
	}

	return localExists, remoteExists, remoteMatchMode, nil
}

func (infos *Infos) logDownloadMemberships(ctx context.Context) {
	if len(infos.UserClients) == 0 || len(infos.Conf.Download.Channels) == 0 {
		return
	}

	accountNames := make([]string, 0, len(infos.UserClients))
	for name := range infos.UserClients {
		accountNames = append(accountNames, name)
	}
	sort.Strings(accountNames)

	for _, task := range infos.Conf.Download.Channels {
		if task.ID == 0 {
			continue
		}
		log.Printf("开始检测频道加入状态: cid: %d", task.ID)

		var wg sync.WaitGroup
		for _, accountName := range accountNames {
			wg.Add(1)
			go func(accountName string, task DownloadChannel) {
				defer wg.Done()

				client := infos.UserClients[accountName]
				if client == nil {
					log.Printf("账号状态: user: %s cid: %d 状态: 无客户端", accountName, task.ID)
					return
				}
				latest, err := infos.getLatestMessageID(client, task.ID)
				if err == nil {
					log.Printf("账号状态: user: %s cid: %d 状态: 已加入 latest: %d", accountName, task.ID, latest)
					return
				}
				log.Printf("账号状态: user: %s cid: %d 状态: 未加入 err: %v", accountName, task.ID, err)
				if infos.Conf.Download.ForceJoin || task.ForceJoin {
					if jerr := tryJoinChannel(client, task.Join); jerr != nil {
						log.Printf("账号状态: user: %s cid: %d 强制加入失败 join: %s err: %v", accountName, task.ID, task.Join, jerr)
						return
					}
					if latest, err = infos.getLatestMessageID(client, task.ID); err == nil {
						log.Printf("账号状态: user: %s cid: %d 强制加入成功 latest: %d", accountName, task.ID, latest)
					} else {
						log.Printf("账号状态: user: %s cid: %d 强制加入后仍不可用 err: %v", accountName, task.ID, err)
					}
				}
			}(accountName, task)
		}
		wg.Wait()
	}
}

func resolveMessageFetchMode(infos *Infos, typeFilter map[string]struct{}, allowAll bool) string {
	mode := "auto"
	if infos != nil && infos.Conf != nil {
		mode = strings.ToLower(strings.TrimSpace(infos.Conf.Download.FetchMode))
	}
	switch mode {
	case "ids", "history", "search":
		if mode == "search" && len(buildMediaSearchFilters(typeFilter, allowAll)) == 0 {
			return "history"
		}
		return mode
	default:
		// auto: 有明确媒体类型时走 search，否则 history
		if len(buildMediaSearchFilters(typeFilter, allowAll)) > 0 {
			return "search"
		}
		return "history"
	}
}

func buildMediaSearchFilters(typeFilter map[string]struct{}, allowAll bool) []telegram.MessagesFilter {
	if allowAll || len(typeFilter) == 0 {
		return nil
	}
	_, wantVideo := typeFilter["video"]
	_, wantPhoto := typeFilter["photo"]
	_, wantDoc := typeFilter["document"]

	filters := make([]telegram.MessagesFilter, 0, 3)
	// photo+video 可合并为一次 PhotoVideo 搜索，减少 RPC
	if wantVideo && wantPhoto {
		filters = append(filters, &telegram.InputMessagesFilterPhotoVideo{})
		wantVideo = false
		wantPhoto = false
	}
	if wantVideo {
		filters = append(filters, &telegram.InputMessagesFilterVideo{})
	}
	if wantPhoto {
		filters = append(filters, &telegram.InputMessagesFilterPhotos{})
	}
	if wantDoc {
		filters = append(filters, &telegram.InputMessagesFilterDocument{})
	}
	return filters
}

func mediaFilterName(filter telegram.MessagesFilter) string {
	if filter == nil {
		return "nil"
	}
	switch filter.(type) {
	case *telegram.InputMessagesFilterVideo:
		return "video"
	case *telegram.InputMessagesFilterPhotos:
		return "photos"
	case *telegram.InputMessagesFilterPhotoVideo:
		return "photo_video"
	case *telegram.InputMessagesFilterDocument:
		return "document"
	case *telegram.InputMessagesFilterEmpty:
		return "empty"
	default:
		return fmt.Sprintf("%T", filter)
	}
}

func detectMessageType(msg telegram.NewMessage) (string, bool) {
	switch {
	case msg.Video() != nil:
		return "video", true
	case msg.Photo() != nil:
		return "photo", true
	case msg.Document() != nil:
		return "document", true
	default:
		return "", false
	}
}

func normalizeTypeFilter(globalTypes, localTypes []string) (map[string]struct{}, bool) {
	effective := globalTypes
	if len(localTypes) > 0 {
		effective = localTypes
	}
	if len(effective) == 0 {
		return nil, true
	}

	set := make(map[string]struct{}, len(effective))
	for _, src := range effective {
		v := strings.ToLower(strings.TrimSpace(src))
		switch v {
		case "", "all", "*":
			return nil, true
		case "video", "photo", "document":
			set[v] = struct{}{}
		}
	}
	if len(set) == 0 {
		return nil, true
	}
	return set, false
}
