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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/amarnathcjd/gogram/telegram"
)

var fileNameSanitizer = regexp.MustCompile(`[<>:"/\\|?*\x00-\x1F]`)

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
				log.Printf("轮询分配: cid=%d -> user=%s (rrIdx=%d)", task.ID, accountName, assignedIdx)
			}
		}

		if client == nil {
			log.Printf("频道下载跳过: cid=%d user=%s, 未找到可用 UserBot", task.ID, task.User)
			continue
		}

		log.Printf("频道开始分配下载: cid=%d user=%s", task.ID, accountName)
		if err := infos.downloadChannelRange(ctx, client, outputRoot, task, sem, &wgFiles, accountName); err != nil {
			log.Printf("频道下载失败: cid=%d err=%v", task.ID, err)
		}
	}
	wgFiles.Wait()
	log.Printf("自动下载任务执行完成")
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
				if err := infos.downloadMessageToFile(ctx, c, outputRoot, m, acct); err != nil {
					log.Printf("下载消息失败: cid=%d mid=%d user=%s err=%v", task.ID, m.ID, acct, err)
				}
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
		for _, accountName := range accountNames {
			client := infos.UserClients[accountName]
			if client == nil {
				log.Printf("账号状态: user=%s cid=%d 状态=无客户端", accountName, task.ID)
				continue
			}
			latest, err := infos.getLatestMessageID(client, task.ID)
			if err == nil {
				log.Printf("账号状态: user=%s cid=%d 状态=已加入 latest=%d", accountName, task.ID, latest)
				continue
			}
			log.Printf("账号状态: user=%s cid=%d 状态=未加入 err=%v", accountName, task.ID, err)
			if infos.Conf.Download.ForceJoin || task.ForceJoin {
				if jerr := tryJoinChannel(client, task.Join); jerr != nil {
					log.Printf("账号状态: user=%s cid=%d 强制加入失败 join=%s err=%v", accountName, task.ID, task.Join, jerr)
					continue
				}
				if latest, err = infos.getLatestMessageID(client, task.ID); err == nil {
					log.Printf("账号状态: user=%s cid=%d 强制加入成功 latest=%d", accountName, task.ID, latest)
				} else {
					log.Printf("账号状态: user=%s cid=%d 强制加入后仍不可用 err=%v", accountName, task.ID, err)
				}
			}
		}
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

	channelName := strings.TrimSpace(msg.Channel.Title)
	if channelName == "" {
		channelName = strconv.FormatInt(msg.ChatID(), 10)
	}
	channelName = sanitizeFileName(channelName)

	content := strings.TrimSpace(msg.Text())
	if content == "" {
		content = strings.TrimSpace(msg.File.Name)
	}
	if content == "" {
		content = "no-title"
	}
	content = sanitizeFileName(content)

	dir := filepath.Join(outputRoot, channelName, fmt.Sprintf("%04d-%02d", msgTime.Year(), msgTime.Month()))
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	// 临时目录用于先写入下载完成的临时文件，之后验证通过再移动到最终位置
	// 使用 files 目录下的 tmp 子目录（infos.FilesPath/tmp）以便与配置/会话文件同目录管理
	tmpDir := filepath.Join(infos.FilesPath, "tmp")
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return err
	}

	ext := determineFileExtension(msg)
	finalPath := filepath.Join(dir, fmt.Sprintf("%d - %s%s", msg.ID, content, ext))
	if _, err := os.Stat(finalPath); err == nil {
		return nil
	}

	// 记录开始下载日志
	tmpPath := filepath.Join(tmpDir, fmt.Sprintf("%d - %s%s.tmp", msg.ID, content, ext))
	log.Printf("开始下载文件: user=%s final=%s", accountName, finalPath)
	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
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

				// 移动到最终位置
				if err := os.Rename(tmpPath, finalPath); err != nil {
					// 若目标存在则尝试删除目标后重命名
					if os.IsExist(err) {
						_ = os.Remove(finalPath)
						if err := os.Rename(tmpPath, finalPath); err != nil {
							return err
						}
					} else {
						return err
					}
				}

				// 对最终文件计算 MD5，用于与远端（如果存在）或临时文件做校验
				finalMD5, err := fileMD5(finalPath)
				if err != nil {
					_ = os.Remove(finalPath)
					return fmt.Errorf("复核下载文件 MD5 失败: %w", err)
				}

				// 尝试从消息元数据获取远端提供的 MD5（字段名可能不同，使用反射尝试多种常见名称）
				expected := getRemoteFileMD5(msg)
				if expected != "" {
					if finalMD5 != expected {
						_ = os.Remove(finalPath)
						return fmt.Errorf("下载文件 MD5 与远端不匹配: expected=%s final=%s", expected, finalMD5)
					}
					log.Printf("下载文件 MD5 校验通过(对比远端): user=%s md5=%s", accountName, finalMD5)
				} else {
					// 若没有远端 MD5，则已按大小校验，记录完成并输出最终 MD5 供诊断
					log.Printf("下载完成(未提供远端 MD5): user=%s md5=%s", accountName, finalMD5)
				}

				log.Printf("下载完成: user=%s path=%s", accountName, finalPath)
				return nil
			}
			timer.Reset(30 * time.Second)
		case <-timer.C:
			_ = os.Remove(tmpPath)
			return fmt.Errorf("下载超时: cid=%d mid=%d", msg.ChatID(), msg.ID)
		}
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

func sanitizeFileName(src string) string {
	src = strings.TrimSpace(src)
	src = strings.ReplaceAll(src, "\n", " ")
	src = fileNameSanitizer.ReplaceAllString(src, "_")
	src = strings.Trim(src, " .")
	src = strings.Join(strings.Fields(src), " ")
	if src == "" {
		return "untitled"
	}
	if len([]rune(src)) > 80 {
		src = string([]rune(src)[:80])
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
