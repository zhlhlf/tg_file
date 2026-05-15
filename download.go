package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
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

	log.Printf("自动下载开始执行, 频道数: %d, 输出目录: %s", len(infos.Conf.Download.Channels), outputRoot)
	infos.logDownloadMemberships(ctx)
	// 并发控制: `concurrent` 限制同时进行的文件下载数量（不是频道）
	concurrency := infos.Conf.Download.Concurrent
	if concurrency <= 0 {
		concurrency = 3
	}
	sem := make(chan struct{}, concurrency)
	var wgFiles sync.WaitGroup
	// 收集已登录的账号列表并排序
	accountNames := make([]string, 0, len(infos.UserClients))
	for name := range infos.UserClients {
		accountNames = append(accountNames, name)
	}
	sort.Strings(accountNames)

	// 准备可用账号列表，用于轮询分配（仅在 task.User 未指定时生效）
	availableAccounts := make([]string, 0, len(accountNames))
	for _, name := range accountNames {
		if infos.UserClients[name] != nil {
			availableAccounts = append(availableAccounts, name)
		}
	}
	rrIdx := 0
	log.Printf("可用账号列表: %v", availableAccounts)

	// 自动解析缺少 ID 的频道 URL
	for i, task := range infos.Conf.Download.Channels {
		log.Printf("频道[%d]: ID=%d, Join=%s", i, task.ID, task.Join)
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
						log.Printf("✅ 自动解析频道 %s 的 ID: %d", channelName, channelID)
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
								log.Printf("账号 %s 已加入频道 cid=%d", an, peerCID)
							} else {
								log.Printf("账号 %s 未加入频道 cid=%d: %v", an, peerCID, err)
								// 若未加入且允许强制加入，则尝试加入
								if infos.Conf.Download.ForceJoin || task.ForceJoin {
									if jerr := tryJoinChannel(c, task.Join); jerr == nil {
										if _, err2 := infos.getLatestMessageID(c, peerCID); err2 == nil {
											log.Printf("账号 %s 成功加入频道 cid=%d", an, peerCID)
										} else {
											log.Printf("账号 %s 加入后仍不可用 cid=%d: %v", an, peerCID, err2)
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

		var accountName string
		var client *telegram.Client

		// 如果配置中指定了特定账号，仍然尊重配置；否则从可用账号中轮询分配
		if strings.TrimSpace(task.User) != "" {
			accountName, client = infos.clientNameForTask(task)
		} else {
			if len(availableAccounts) == 0 {
				accountName, client = infos.clientNameForTask(task)
			} else {
				assignedIdx := rrIdx % len(availableAccounts)
				accountName = availableAccounts[assignedIdx]
				client = infos.UserClients[accountName]
				rrIdx++
			}
		}

		if client == nil {
			log.Printf("频道下载跳过: cid=%d user=%s, 未找到可用 UserBot", task.ID, task.User)
			continue
		}

		if err := infos.downloadChannelRange(ctx, client, outputRoot, task, sem, &wgFiles, accountName); err != nil {
			log.Printf("频道下载失败: cid=%d err=%v", task.ID, err)
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
							log.Printf("发现频道新消息: cid=%d last=%d latest=%d, 开始增量下载", task.ID, last, latest)
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
								log.Printf("增量下载失败: cid=%d err=%v", task.ID, err)
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
			log.Printf("账号 %s 未加入频道 cid=%d, 尝试强制加入", accountName, task.ID)
			if jerr := tryJoinChannel(client, task.Join); jerr == nil {
				latest, err = infos.getLatestMessageID(client, task.ID)
			} else {
				log.Printf("尝试加入频道失败: cid=%d join=%s err=%v", task.ID, task.Join, jerr)
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
		log.Printf("频道无需下载: cid=%d from=%d latest=%d", task.ID, start, latest)
		return nil
	}

	typeFilter, allowAll := normalizeTypeFilter(infos.Conf.Download.GlobalTypes, task.Types)
	log.Printf("频道开始下载: cid=%d from=%d latest=%d", task.ID, start, latest)

	// 为文件级别分配准备可用账号列表（用于按文件轮询分配）
	accountNames := make([]string, 0, len(infos.UserClients))
	for name := range infos.UserClients {
		accountNames = append(accountNames, name)
	}
	sort.Strings(accountNames)
	availableAccounts := make([]string, 0, len(accountNames))
	for _, n := range accountNames {
		if infos.UserClients[n] != nil {
			availableAccounts = append(availableAccounts, n)
		}
	}
	rrIdx := 0

	for cursor := start; cursor <= latest; cursor += 100 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		end := cursor + 99
		if end > latest {
			end = latest
		}

		ids := make([]int32, 0, end-cursor+1)
		for mid := cursor; mid <= end; mid++ {
			ids = append(ids, mid)
		}

		ms, err := client.GetMessages(task.ID, &telegram.SearchOption{IDs: ids})
		if err != nil {
			log.Printf("批量获取消息失败: cid=%d start=%d end=%d err=%v", task.ID, cursor, end, err)
			continue
		}
		if len(ms) == 0 {
			continue
		}

		sort.Slice(ms, func(i, j int) bool { return ms[i].ID < ms[j].ID })
		for _, msg := range ms {
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

			// 选择用于此文件下载的账号（若 task.User 指定则使用该账号），否则按文件轮询分配
			var fileAccount string
			var fileClient *telegram.Client
			if strings.TrimSpace(task.User) != "" {
				fileAccount, fileClient = infos.clientNameForTask(task)
			} else {
				if len(availableAccounts) == 0 {
					fileAccount, fileClient = infos.clientNameForTask(task)
				} else {
					fileAccount = availableAccounts[rrIdx%len(availableAccounts)]
					fileClient = infos.UserClients[fileAccount]
					rrIdx++
				}
			}

			// 启动受限并发的文件下载任务（文件级并发由 sem 控制）
			wgFiles.Add(1)
			sem <- struct{}{}
			go func(m telegram.NewMessage, c *telegram.Client, acct string) {
				defer wgFiles.Done()
				defer func() { <-sem }()
				if c == nil {
					log.Printf("下载消息失败: cid=%d mid=%d user=%s err=%v", task.ID, m.ID, acct, fmt.Errorf("未找到可用客户端"))
					return
				}
				var err error
				const maxAttempts = 3
				for attempt := 1; attempt <= maxAttempts; attempt++ {
					err = infos.downloadMessageToFile(ctx, c, outputRoot, m, acct)
					if err == nil {
						return
					}
					if attempt < maxAttempts {
						log.Printf("下载消息失败，准备重试: cid=%d mid=%d user=%s attempt=%d/%d err=%v", task.ID, m.ID, acct, attempt, maxAttempts, err)
						time.Sleep(time.Duration(attempt) * 2 * time.Second)
					}
				}
				log.Printf("下载消息失败: cid=%d mid=%d user=%s err=%v", task.ID, m.ID, acct, err)
			}(msg, fileClient, fileAccount)
		}
	}
	return nil
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
		log.Printf("开始检测频道加入状态: cid=%d", task.ID)
		
		var wg sync.WaitGroup
		for _, accountName := range accountNames {
			wg.Add(1)
			go func(accountName string, task DownloadChannel) {
				defer wg.Done()
				
				client := infos.UserClients[accountName]
				if client == nil {
					log.Printf("账号状态: user=%s cid=%d 状态=无客户端", accountName, task.ID)
					return
				}
				latest, err := infos.getLatestMessageID(client, task.ID)
				if err == nil {
					log.Printf("账号状态: user=%s cid=%d 状态=已加入 latest=%d", accountName, task.ID, latest)
					return
				}
				log.Printf("账号状态: user=%s cid=%d 状态=未加入 err=%v", accountName, task.ID, err)
				if infos.Conf.Download.ForceJoin || task.ForceJoin {
					if jerr := tryJoinChannel(client, task.Join); jerr != nil {
						log.Printf("账号状态: user=%s cid=%d 强制加入失败 join=%s err=%v", accountName, task.ID, task.Join, jerr)
						return
					}
					if latest, err = infos.getLatestMessageID(client, task.ID); err == nil {
						log.Printf("账号状态: user=%s cid=%d 强制加入成功 latest=%d", accountName, task.ID, latest)
					} else {
						log.Printf("账号状态: user=%s cid=%d 强制加入后仍不可用 err=%v", accountName, task.ID, err)
					}
				}
			}(accountName, task)
		}
		wg.Wait()
	}
}

func (infos *Infos) getLatestMessageID(client *telegram.Client, cid int64) (int32, error) {
	ms, err := client.GetMessages(cid, &telegram.SearchOption{Limit: 1})
	if err != nil {
		return 0, err
	}
	if len(ms) == 0 {
		return 0, nil
	}
	return ms[0].ID, nil
}

func (infos *Infos) downloadMessageToFile(ctx context.Context, client *telegram.Client, outputRoot string, msg telegram.NewMessage, accountName string) error {
	msgTime := time.Now()
	if msg.Message != nil && msg.Message.Date != 0 {
		msgTime = time.Unix(int64(msg.Message.Date), 0)
	}

	rawText := extractMessageContent(msg)
	if strings.TrimSpace(rawText) == "" {
		if groupCaption, err := infos.getMediaGroupCaption(ctx, client, msg); err != nil {
			log.Printf("消息组 caption 获取失败: cid=%d mid=%d err=%v", msg.ChatID(), msg.ID, err)
		} else if strings.TrimSpace(groupCaption) != "" {
			rawText = groupCaption
			debugf("消息组 caption 命中: cid=%d mid=%d caption=%q", msg.ChatID(), msg.ID, rawText)
		}
	}
	debugf("原始消息内容: cid=%d mid=%d caption=%q fileName=%q", msg.ChatID(), msg.ID, rawText, func() string {
		if msg.File != nil {
			return msg.File.Name
		}
		return ""
	}())

	channelName := strings.TrimSpace(msg.Channel.Title)
	if channelName == "" {
		channelName = strconv.FormatInt(msg.ChatID(), 10)
	}
	channelName = sanitizeFileName(channelName)

	content := strings.TrimSpace(rawText)
	hasContent := content != ""
	content = sanitizeFileName(content)

	ext := determineFileExtension(msg)
	dir := filepath.Join(outputRoot, channelName, fmt.Sprintf("%04d_%02d", msgTime.Year(), msgTime.Month()))
	fileName := fmt.Sprintf("%d%s", msg.ID, ext)
	if hasContent && content != "" {
		fileName = fmt.Sprintf("%d - %s%s", msg.ID, content, ext)
	}
	if infos != nil && infos.Conf != nil {
		fileNameLower := strings.ToLower(fileName)
		for _, rawKeyword := range infos.Conf.Download.SkipNameContains {
			keyword := strings.TrimSpace(rawKeyword)
			if keyword == "" {
				continue
			}
			if strings.Contains(fileNameLower, strings.ToLower(keyword)) {
				log.Printf("命中过滤规则, 跳过下载: user=%s file=%s filter=%q", accountName, fileName, keyword)
				return nil
			}
		}
	}
	finalPath := filepath.Join(dir, fileName)
	displayLocalPath := func(path string) string {
		cleanPath := filepath.Clean(path)
		cleanRoot := filepath.Clean(outputRoot)
		if relPath, relErr := filepath.Rel(cleanRoot, cleanPath); relErr == nil && relPath != "." && !strings.HasPrefix(relPath, "..") {
			return relPath
		}
		rootWithSep := cleanRoot + string(os.PathSeparator)
		if strings.HasPrefix(cleanPath, rootWithSep) {
			return strings.TrimPrefix(cleanPath, rootWithSep)
		}
		return filepath.Base(cleanPath)
	}

	if infos != nil && infos.Conf != nil && infos.Conf.Download.Rclone.Enabled {
		if remotePath, remoteErr := infos.rcloneRemotePath(outputRoot, finalPath); remoteErr == nil {
			debugf("检查远端文件是否存在: path=%s remote=%s", displayLocalPath(finalPath), remotePath)
		} else {
			debugf("检查文件是否存在: path=%s", displayLocalPath(finalPath))
		}
		if exists, err := infos.rcloneFileExists(ctx, outputRoot, finalPath); err != nil {
			log.Printf("rclone 文件检查失败: path=%s err=%v", displayLocalPath(finalPath), err)
		} else if exists {
			log.Printf("rclone中存在, 跳过: path=%s", displayLocalPath(finalPath))
			return nil
		}
	}
	if _, err := os.Stat(finalPath); err == nil {
		if infos != nil && infos.Conf != nil && infos.Conf.Download.Rclone.Enabled {
			remotePath, rcloneErr := infos.rcloneRemotePath(outputRoot, finalPath)
			if rcloneErr != nil {
				return rcloneErr
			}
			mode := infos.rcloneTransferMode()
			log.Printf("本地文件已存在，执行 rclone %s: user=%s path=%s", mode, accountName, displayLocalPath(finalPath))
			if rcloneErr := infos.rcloneTransferFile(ctx, finalPath, remotePath, mode); rcloneErr != nil {
				return rcloneErr
			}
			log.Printf("下载完成: %s", displayLocalPath(finalPath))
			log.Printf("rclone %s 完成: %s", mode, displayLocalPath(finalPath))
		}
		return nil
	}

	// 临时目录用于先写入下载完成的临时文件，之后验证通过再移动到最终位置
	// 使用 files 目录下的 tmp 子目录（infos.FilesPath/tmp）以便与配置/会话文件同目录管理
	tmpDir := filepath.Join(infos.FilesPath, "tmp")
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return err
	}

	// 记录开始下载日志
	tmpFileName := fmt.Sprintf("%d%s.tmp", msg.ID, ext)
	if hasContent && content != "" {
		tmpFileName = fmt.Sprintf("%d - %s%s.tmp", msg.ID, content, ext)
	}
	tmpPath := filepath.Join(tmpDir, tmpFileName)
	log.Printf("下载文件: user=%s final=%s", accountName, displayLocalPath(finalPath))
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	success := false
	defer func() {
		if !success {
			_ = os.Remove(tmpPath)
		}
	}()
	// 在后续逻辑中会关闭 f

	fileCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 每个文件的并发 workers 优先使用 download.fileWorkers, 否则回退到全局配置
	workers := infos.Conf.Download.FileWorkers
	if workers <= 0 {
		workers = infos.Conf.Workers
	}
	if workers <= 0 {
		workers = 1
	}
	stream := newStream(fileCtx, client, msg.Media(), workers, msg.ID, msg.ChatID(), msg.File.Size, msg.File.Name)
	if err := stream.warmConnection(fileCtx); err != nil {
		_ = f.Close()
		return err
	}
	go stream.start(0, msg.File.Size-1)
	defer func() {
		stream.clean()
		_ = f.Close()
	}()

	timer := time.NewTimer(120 * time.Second)
	defer timer.Stop()

	// 统计写入速度, 每秒输出一次 (仅 debug 模式)
	var totalWritten int64
	lastWritten := int64(0)
	tick := time.NewTicker(1 * time.Second)
	defer tick.Stop()

	for {
		select {
		case <-fileCtx.Done():
			return fileCtx.Err()
		case <-tick.C:
			if infos != nil && infos.Conf != nil && infos.Conf.Debug {
				delta := totalWritten - lastWritten
				lastWritten = totalWritten
				debugf("下载速度: cid=%d mid=%d %s/s (written=%d)", msg.ChatID(), msg.ID, humanizeBytes(delta), totalWritten)
			}
		case task := <-stream.Tasks:
			if task == nil {
				continue
			}
			if task.Error != nil {
				return task.Error
			}

			contentBytes, ok := <-task.Content
			if !ok {
				return nil
			}
			n, err := f.Write(contentBytes)
			if err != nil {
				return err
			}
			totalWritten += int64(n)

			if task.ContentEnd >= msg.File.Size-1 {
				// flush and close before verification
				if err := f.Sync(); err != nil {
					debugf("文件同步失败: %v", err)
				}
				if err := f.Close(); err != nil {
					debugf("关闭临时文件失败: %v", err)
				}

				// 验证文件大小
				fi, statErr := os.Stat(tmpPath)
				if statErr != nil {
					_ = os.Remove(tmpPath)
					return statErr
				}
				if msg.File != nil && msg.File.Size > 0 {
					if fi.Size() != msg.File.Size {
						// 清理临时文件
						_ = os.Remove(tmpPath)
						return fmt.Errorf("文件大小校验失败: 期望 %d, 实际 %d", msg.File.Size, fi.Size())
					}
				}

				// (不再对临时文件与最终文件做 MD5 比对)

				// 对临时文件计算 MD5，用于与远端（如果存在）或临时文件做校验
				finalMD5, err := fileMD5(tmpPath)
				if err != nil {
					_ = os.Remove(tmpPath)
					return fmt.Errorf("复核下载文件 MD5 失败: %w", err)
				}

				// 尝试从消息元数据获取远端提供的 MD5（字段名可能不同，使用反射尝试多种常见名称）
				expected := getRemoteFileMD5(msg)
				if expected != "" {
					if finalMD5 != expected {
						_ = os.Remove(tmpPath)
						return fmt.Errorf("下载文件 MD5 与远端不匹配: expected=%s final=%s", expected, finalMD5)
					}
					log.Printf("下载文件 MD5 校验通过(对比远端): user=%s md5=%s", accountName, finalMD5)
				}

				if infos != nil && infos.Conf != nil && infos.Conf.Download.Rclone.Enabled {
					remotePath, err := infos.rcloneRemotePath(outputRoot, finalPath)
					if err != nil {
						return err
					}
					mode := infos.rcloneTransferMode()
					if err := os.MkdirAll(dir, 0755); err != nil {
						return err
					}
					if err := os.Rename(tmpPath, finalPath); err != nil {
						if os.IsExist(err) {
							_ = os.Remove(finalPath)
							if err := os.Rename(tmpPath, finalPath); err != nil {
								return err
							}
						} else {
							return err
						}
					}
					log.Printf("下载完成: %s", displayLocalPath(finalPath))
					if err := infos.rcloneTransferFile(ctx, finalPath, remotePath, mode); err != nil {
						return err
					}
					log.Printf("rclone %s 完成: %s", mode, displayLocalPath(finalPath))
					success = true
					return nil
				}

				if err := os.MkdirAll(dir, 0755); err != nil {
					return err
				}
				if err := os.Rename(tmpPath, finalPath); err != nil {
					if os.IsExist(err) {
						_ = os.Remove(finalPath)
						if err := os.Rename(tmpPath, finalPath); err != nil {
							return err
						}
					} else {
						return err
					}
				}

				log.Printf("下载完成: %s", displayLocalPath(finalPath))
				success = true
				return nil
			}
			timer.Reset(30 * time.Second)
		case <-timer.C:
			_ = os.Remove(tmpPath)
			return fmt.Errorf("下载超时: cid=%d mid=%d", msg.ChatID(), msg.ID)
		}
	}
}

func (infos *Infos) rcloneRemotePath(outputRoot, finalPath string) (string, error) {
	if infos == nil || infos.Conf == nil {
		return "", nil
	}
	rcloneConf := infos.Conf.Download.Rclone
	remoteRoot := strings.TrimSpace(rcloneConf.Remote)
	if remoteRoot == "" {
		return "", fmt.Errorf("rclone 已启用但未配置 remote")
	}
	relPath, err := filepath.Rel(outputRoot, finalPath)
	if err != nil {
		return "", err
	}
	return joinRclonePath(remoteRoot, filepath.ToSlash(relPath)), nil
}

func (infos *Infos) rcloneFileExists(ctx context.Context, outputRoot, finalPath string) (bool, error) {
	if infos == nil || infos.Conf == nil {
		return false, nil
	}
	rcloneConf := infos.Conf.Download.Rclone
	if !rcloneConf.Enabled {
		return false, nil
	}
	remoteRoot := strings.TrimSpace(rcloneConf.Remote)
	if remoteRoot == "" {
		return false, fmt.Errorf("rclone 已启用但未配置 remote")
	}
	relPath, err := filepath.Rel(outputRoot, finalPath)
	if err != nil {
		return false, err
	}
	remotePath := joinRclonePath(remoteRoot, filepath.ToSlash(relPath))
	args := infos.rcloneArgs("lsjson", "--stat", remotePath)
	cmd := exec.CommandContext(ctx, "rclone", args...)
	if output, err := cmd.CombinedOutput(); err != nil {
		text := strings.TrimSpace(string(output))
		if strings.Contains(strings.ToLower(text), "directory not found") {
			return false, nil
		}
		if text != "" {
			return false, fmt.Errorf("%w: %s", err, text)
		}
		return false, err
	}
	return true, nil
}

func (infos *Infos) rcloneMoveFile(ctx context.Context, localPath, remotePath string) error {
	if infos == nil || infos.Conf == nil {
		return nil
	}
	rcloneConf := infos.Conf.Download.Rclone
	if !rcloneConf.Enabled {
		return nil
	}
	args := infos.rcloneArgs("moveto", localPath, remotePath)
	cmd := exec.CommandContext(ctx, "rclone", args...)
	if output, err := cmd.CombinedOutput(); err != nil {
		text := strings.TrimSpace(string(output))
		if text != "" {
			return fmt.Errorf("%w: %s", err, text)
		}
		return err
	}
	return nil
}

func (infos *Infos) rcloneTransferFile(ctx context.Context, localPath, remotePath, mode string) error {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "copy":
		return infos.rcloneCopyFile(ctx, localPath, remotePath)
	default:
		return infos.rcloneMoveFile(ctx, localPath, remotePath)
	}
}

func (infos *Infos) rcloneCopyFile(ctx context.Context, localPath, remotePath string) error {
	if infos == nil || infos.Conf == nil {
		return nil
	}
	rcloneConf := infos.Conf.Download.Rclone
	if !rcloneConf.Enabled {
		return nil
	}
	args := infos.rcloneArgs("copyto", localPath, remotePath)
	cmd := exec.CommandContext(ctx, "rclone", args...)
	if output, err := cmd.CombinedOutput(); err != nil {
		text := strings.TrimSpace(string(output))
		if text != "" {
			return fmt.Errorf("%w: %s", err, text)
		}
		return err
	}
	return nil
}

func (infos *Infos) rcloneArgs(extra ...string) []string {
	args := make([]string, 0, len(extra)+2)
	rcloneConf := infos.Conf.Download.Rclone
	configFile := strings.TrimSpace(rcloneConf.ConfigFile)
	if configFile != "" {
		args = append(args, "--config", configFile)
	}
	args = append(args, extra...)
	return args
}

func (infos *Infos) rcloneTransferMode() string {
	if infos == nil || infos.Conf == nil {
		return "move"
	}
	mode := strings.ToLower(strings.TrimSpace(infos.Conf.Download.Rclone.TransferMode))
	if mode == "copy" {
		return "copy"
	}
	return "move"
}

func joinRclonePath(base, rel string) string {
	base = strings.TrimSpace(base)
	rel = strings.TrimLeft(strings.TrimSpace(rel), "/")
	if base == "" {
		return rel
	}
	if rel == "" {
		return base
	}
	if strings.HasSuffix(base, ":") {
		return base + rel
	}
	return strings.TrimRight(base, "/") + "/" + rel
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

func extractMessageContent(msg telegram.NewMessage) string {
	if msg.Message == nil {
		return strings.TrimSpace(msg.Text())
	}
	for _, fieldName := range []string{"Caption"} {
		if text := strings.TrimSpace(readStringField(msg.Message, fieldName)); text != "" {
			return text
		}
	}
	return strings.TrimSpace(msg.Text())
}

func (infos *Infos) getMediaGroupCaption(ctx context.Context, client *telegram.Client, msg telegram.NewMessage) (string, error) {
	if client == nil || msg.Message == nil || msg.Message.GroupedID == 0 {
		return "", nil
	}

	ids := make([]int32, 0, 11)
	seen := make(map[int32]struct{}, 11)
	for offset := int32(-5); offset <= 5; offset++ {
		id := msg.ID + offset
		if id <= 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return "", nil
	}

	ms, err := client.GetMessages(msg.ChatID(), &telegram.SearchOption{IDs: ids})
	if err != nil {
		return "", err
	}
	for _, groupMsg := range ms {
		if groupMsg.Message == nil || groupMsg.Message.GroupedID != msg.Message.GroupedID {
			continue
		}
		caption := strings.TrimSpace(extractMessageContent(groupMsg))
		if caption != "" {
			return caption, nil
		}
	}
	return "", nil
}

func readStringField(src any, fieldName string) string {
	v := reflect.ValueOf(src)
	if !v.IsValid() {
		return ""
	}
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return ""
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return ""
	}
	f := v.FieldByName(fieldName)
	if !f.IsValid() || f.Kind() != reflect.String {
		return ""
	}
	return f.String()
}

func sanitizeFileName(src string) string {
	// src = strings.TrimSpace(src)
	src = strings.ReplaceAll(src, "\n", "_")
	src = strings.ReplaceAll(src, "\r", "_")
	if src == "" {
		return "untitled"
	}
	return src
}

func determineFileExtension(msg telegram.NewMessage) string {
	// Prefer explicit file name extension when available
	if msg.File != nil && msg.File.Name != "" {
		if ext := filepath.Ext(msg.File.Name); ext != "" {
			return ext
		}
	}
	// Fallback by message type
	if msg.Video() != nil {
		return ".mp4"
	}
	if msg.Photo() != nil {
		return ".jpg"
	}
	if msg.Document() != nil {
		// try to use document mime/name, else generic
		return ".bin"
	}
	return ".bin"
}

// getRemoteFileMD5 尝试从消息的 File 元数据中提取远端提供的 MD5 字符串。
// 使用反射尝试若干常见字段名或 map。若未找到返回空字符串。
func getRemoteFileMD5(msg telegram.NewMessage) string {
	if msg.File == nil {
		return ""
	}
	v := reflect.ValueOf(msg.File)
	if !v.IsValid() {
		return ""
	}
	if v.Kind() == reflect.Ptr {
		if v.IsNil() {
			return ""
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return ""
	}

	// 常见字段名
	candidates := []string{"MD5", "Md5", "Md5Hash", "Hash", "Checksum", "Sha1", "Sha256"}
	for _, name := range candidates {
		f := v.FieldByName(name)
		if f.IsValid() && f.Kind() == reflect.String {
			s := strings.TrimSpace(f.String())
			if s != "" {
				return s
			}
		}
	}

	// 若存在 map[string]string 类型的字段（例如 Hashes），尝试查找 md5 相关键
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		if !f.IsValid() {
			continue
		}
		if f.Kind() == reflect.Map {
			// 仅处理 map[string]string
			if f.Type().Key().Kind() == reflect.String && f.Type().Elem().Kind() == reflect.String {
				for _, key := range f.MapKeys() {
					k := strings.ToLower(strings.TrimSpace(key.String()))
					if strings.Contains(k, "md5") || strings.Contains(k, "md-5") || strings.Contains(k, "checksum") {
						val := f.MapIndex(key)
						if val.IsValid() && val.Kind() == reflect.String {
							s := strings.TrimSpace(val.String())
							if s != "" {
								return s
							}
						}
					}
				}
			}
		}
	}

	return ""
}
