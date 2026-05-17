// userbots - 下载

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
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
	var downloadPeer any
	if downloadMsg.Message != nil && downloadMsg.Message.PeerID != nil {
		downloadPeer = downloadMsg.Message.PeerID
	}
	stream := newStream(fileCtx, downloadClient, downloadMsg.Media(), workers, downloadMsg.ID, downloadMsg.ChatID(), downloadMsg.File.Size, downloadMsg.File.Name, downloadPeer, false)
	if err := stream.warmConnection(fileCtx); err != nil {
		_ = f.Close()
		return err
	}
	go stream.start(0, downloadMsg.File.Size-1)
	defer func() {
		stream.clean()
		_ = f.Close()
	}()

	timer := time.NewTimer(120 * time.Second)
	defer timer.Stop()

	var totalWritten int64
	lastWritten := int64(0)
	lastSpeedAt := time.Now()
	startedAt := lastSpeedAt
	botCaption := strings.TrimSpace(extractMessageContent(downloadMsg))
	botLabel := strings.TrimSpace(accountName)
	if parts := strings.Split(botLabel, "->"); len(parts) > 1 {
		candidate := strings.TrimSpace(parts[len(parts)-1])
		if candidate != "" {
			botLabel = candidate
		}
	}
	tick := time.NewTicker(1 * time.Second)
	defer tick.Stop()

	for {
		select {
		case <-fileCtx.Done():
			return fileCtx.Err()
		case <-tick.C:
			if infos != nil && infos.Conf != nil && infos.Conf.Debug {
				now := time.Now()
				deltaBytes := totalWritten - lastWritten
				deltaSec := now.Sub(lastSpeedAt).Seconds()
				if deltaSec > 0 {
					curRate := float64(deltaBytes) / deltaSec
					avgSec := now.Sub(startedAt).Seconds()
					avgRate := 0.0
					if avgSec > 0 {
						avgRate = float64(totalWritten) / avgSec
					}
					debugf("下载速度: bot=%s cap=%q cur=%s/s avg=%s/s (written=%d)", botLabel, botCaption, formatRate(curRate), formatRate(avgRate), totalWritten)
				}
				lastWritten = totalWritten
				lastSpeedAt = now
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

			if task.ContentEnd >= downloadMsg.File.Size-1 {
				dir := filepath.Dir(targetInfo.FinalPath)
				if err := f.Sync(); err != nil {
					debugf("文件同步失败: %v", err)
				}
				if err := f.Close(); err != nil {
					debugf("关闭临时文件失败: %v", err)
				}

				fi, statErr := os.Stat(tmpPath)
				if statErr != nil {
					_ = os.Remove(tmpPath)
					return statErr
				}
				if downloadMsg.File != nil && downloadMsg.File.Size > 0 {
					if fi.Size() != downloadMsg.File.Size {
						_ = os.Remove(tmpPath)
						return fmt.Errorf("文件大小校验失败: 期望 %d, 实际 %d", downloadMsg.File.Size, fi.Size())
					}
				}
				if err := infos.verifyDownloadedFileHashes(sourceClient, sourceMsg, tmpPath); err != nil {
					_ = os.Remove(tmpPath)
					return err
				}

				// Telegram 文件元数据通常不提供稳定可用的 MD5/SHA。
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
			timer.Reset(30 * time.Second)
		case <-timer.C:
			_ = os.Remove(tmpPath)
			return fmt.Errorf("下载超时: cid=%d mid=%d", downloadMsg.ChatID(), downloadMsg.ID)
		}
	}
}
