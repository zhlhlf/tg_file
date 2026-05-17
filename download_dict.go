// userbots - 下载

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/amarnathcjd/gogram/telegram"
)

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

func (infos *Infos) downloadMessage(ctx context.Context, sourceClient *telegram.Client, downloadClient *telegram.Client, outputRoot string, sourceMsg telegram.NewMessage, downloadMsg telegram.NewMessage, accountName string, relayCounter *uint64, cache *mediaResolveCache) error {
	if len(infos.RelayBotClients) > 0 && relayCounter != nil {
		return infos.downloadMessageViaRelay(ctx, sourceClient, outputRoot, sourceMsg, accountName, relayCounter, cache)
	}
	return infos.downloadMessageToFile(ctx, sourceClient, downloadClient, outputRoot, sourceMsg, downloadMsg, accountName, cache)
}

func (infos *Infos) downloadMessageToFile(ctx context.Context, sourceClient *telegram.Client, downloadClient *telegram.Client, outputRoot string, sourceMsg telegram.NewMessage, downloadMsg telegram.NewMessage, accountName string, cache *mediaResolveCache) error {
	targetInfo, err := infos.resolveMediaTarget(ctx, sourceClient, outputRoot, sourceMsg, cache)
	if err != nil {
		return err
	}
	if infos.shouldSkipByFileName(targetInfo.FileName, targetInfo.FinalPath) {
		return nil
	}
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
		if remotePath, remoteErr := infos.rcloneRemotePath(outputRoot, targetInfo.FinalPath); remoteErr == nil {
			debugf("检查远端文件是否存在: path=%s remote=%s", displayLocalPath(targetInfo.FinalPath), remotePath)
		} else {
			debugf("检查文件是否存在: path=%s", displayLocalPath(targetInfo.FinalPath))
		}
	}
	if handled, err := infos.ensureExistingMediaTarget(ctx, outputRoot, targetInfo.FinalPath); err != nil {
		return err
	} else if handled {
		return nil
	}

	tmpDir := filepath.Join(infos.FilesPath, "tmp")
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return err
	}

	tmpFileName := fmt.Sprintf("%d%s.tmp", sourceMsg.ID, targetInfo.Ext)
	if targetInfo.HasContent && targetInfo.Content != "" {
		tmpFileName = fmt.Sprintf("%d - %s%s.tmp", sourceMsg.ID, targetInfo.Content, targetInfo.Ext)
	}
	tmpPath := filepath.Join(tmpDir, tmpFileName)
	log.Printf("下载文件: user=%s final=%s", accountName, displayLocalPath(targetInfo.FinalPath))
	success := false
	defer func() {
		if !success {
			_ = os.Remove(tmpPath)
		}
	}()

	fileCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	workers := infos.Conf.Download.FileWorkers
	if workers <= 0 {
		workers = infos.Conf.Workers
	}
	if workers <= 0 {
		workers = 1
	}
	if downloadMsg.File == nil || downloadMsg.Media() == nil {
		return fmt.Errorf("下载消息缺少文件信息: sourceCid=%d sourceMid=%d downloadCid=%d downloadMid=%d", sourceMsg.ChatID(), sourceMsg.ID, downloadMsg.ChatID(), downloadMsg.ID)
	}
	botCaption := strings.TrimSpace(extractMessageContent(downloadMsg))
	botLabel := strings.TrimSpace(accountName)
	if parts := strings.Split(botLabel, "->"); len(parts) > 1 {
		candidate := strings.TrimSpace(parts[len(parts)-1])
		if candidate != "" {
			botLabel = candidate
		}
	}
	lastProgressAt := time.Time{}
	var lastSizeChangeUnix atomic.Int64
	lastSizeChangeUnix.Store(time.Now().UnixNano())
	var lastObservedSize atomic.Int64
	lastObservedSize.Store(-1)
	var noSizeChangeAbort atomic.Bool
	progressCallback := func(info *telegram.ProgressInfo) {
		if info == nil {
			return
		}
		speedText := strings.TrimSpace(info.SpeedString())
		if infos == nil || infos.Conf == nil || !infos.Conf.Debug {
			return
		}
		now := time.Now()
		if !lastProgressAt.IsZero() && now.Sub(lastProgressAt) < time.Second {
			return
		}
		lastProgressAt = now
		debugf("下载进度: bot=%s cap=%q progress=%.2f%% speed=%s eta=%s", botLabel, botCaption, info.Percentage, speedText, info.ETAString())
	}
	watchdogDone := make(chan struct{})
	defer close(watchdogDone)
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-watchdogDone:
				return
			case <-fileCtx.Done():
				return
			case <-ticker.C:
				currentSize := int64(-1)
				if fi, statErr := os.Stat(tmpPath); statErr == nil {
					currentSize = fi.Size()
				}
				if currentSize != lastObservedSize.Load() {
					lastObservedSize.Store(currentSize)
					lastSizeChangeUnix.Store(time.Now().UnixNano())
				}
				lastAt := time.Unix(0, lastSizeChangeUnix.Load())
				if time.Since(lastAt) < 20*time.Second {
					continue
				}
				if noSizeChangeAbort.CompareAndSwap(false, true) {
					log.Printf("下载停滞，20秒内文件大小无变化，取消本次下载: bot=%s cap=%q size=%d sourceCid=%d sourceMid=%d downloadCid=%d downloadMid=%d", botLabel, botCaption, currentSize, sourceMsg.ChatID(), sourceMsg.ID, downloadMsg.ChatID(), downloadMsg.ID)
					cancel()
				}
				return
			}
		}
	}()
	debugf("开始 DownloadMedia: bot=%s cap=%q threads=%d sourceCid=%d sourceMid=%d downloadCid=%d downloadMid=%d", botLabel, botCaption, workers, sourceMsg.ChatID(), sourceMsg.ID, downloadMsg.ChatID(), downloadMsg.ID)
	_, err = downloadClient.DownloadMedia(downloadMsg.Media(), &telegram.DownloadOptions{
		FileName:         tmpPath,
		Threads:          workers,
		Ctx:              fileCtx,
		ProgressCallback: progressCallback,
		ProgressInterval: 1,
	})
	debugf("DownloadMedia 返回: bot=%s cap=%q err=%v sourceCid=%d sourceMid=%d downloadCid=%d downloadMid=%d", botLabel, botCaption, err, sourceMsg.ChatID(), sourceMsg.ID, downloadMsg.ChatID(), downloadMsg.ID)
	if err != nil {
		if noSizeChangeAbort.Load() {
			return fmt.Errorf("下载停滞: 20秒内文件大小无变化，交由上层重试: sourceCid=%d sourceMid=%d downloadCid=%d downloadMid=%d", sourceMsg.ChatID(), sourceMsg.ID, downloadMsg.ChatID(), downloadMsg.ID)
		}
		return fmt.Errorf("下载失败: sourceCid=%d sourceMid=%d downloadCid=%d downloadMid=%d: %w", sourceMsg.ChatID(), sourceMsg.ID, downloadMsg.ChatID(), downloadMsg.ID, err)
	}

	dir := filepath.Dir(targetInfo.FinalPath)
	fi, statErr := os.Stat(tmpPath)
	if statErr != nil {
		return statErr
	}
	if downloadMsg.File != nil && downloadMsg.File.Size > 0 {
		if fi.Size() != downloadMsg.File.Size {
			return fmt.Errorf("文件大小校验失败: 期望 %d, 实际 %d", downloadMsg.File.Size, fi.Size())
		}
	}

	// 这里只做大小校验，避免误判导致有效文件被删除。

	if infos != nil && infos.Conf != nil && infos.Conf.Download.Rclone.Enabled {
		remotePath, err := infos.rcloneRemotePath(outputRoot, targetInfo.FinalPath)
		if err != nil {
			return err
		}
		mode := infos.rcloneTransferMode()
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
		if err := os.Rename(tmpPath, targetInfo.FinalPath); err != nil {
			if os.IsExist(err) {
				_ = os.Remove(targetInfo.FinalPath)
				if err := os.Rename(tmpPath, targetInfo.FinalPath); err != nil {
					return err
				}
			} else {
				return err
			}
		}
		log.Printf("下载完成: %s", displayLocalPath(targetInfo.FinalPath))
		if err := infos.rcloneTransferFile(ctx, targetInfo.FinalPath, remotePath, mode); err != nil {
			return err
		}
		log.Printf("rclone %s 完成: %s", mode, displayLocalPath(targetInfo.FinalPath))
		success = true
		return nil
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, targetInfo.FinalPath); err != nil {
		if os.IsExist(err) {
			_ = os.Remove(targetInfo.FinalPath)
			if err := os.Rename(tmpPath, targetInfo.FinalPath); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	log.Printf("下载完成: %s", displayLocalPath(targetInfo.FinalPath))
	success = true
	return nil
}
