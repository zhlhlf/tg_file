package main

import (
	"context"
	"fmt"
	"path/filepath"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/amarnathcjd/gogram/telegram"
)

type mediaTargetInfo struct {
	MsgTime     time.Time
	ChannelName string
	RawText     string
	Content     string
	HasContent  bool
	Ext         string
	FileName    string
	FinalPath   string
}

func buildMediaTargetPath(outputRoot, channelName string, msgTime time.Time, fileName string) string {
	channelName = sanitizeFileName(strings.TrimSpace(channelName))
	return filepath.Join(outputRoot, channelName, fmt.Sprintf("%04d_%02d", msgTime.Year(), msgTime.Month()), fileName)
}

func (infos *Infos) resolveMediaTarget(ctx context.Context, sourceClient *telegram.Client, outputRoot string, sourceMsg telegram.NewMessage) (mediaTargetInfo, error) {
	info := mediaTargetInfo{}

	msgTime := time.Now()
	if sourceMsg.Message != nil && sourceMsg.Message.Date != 0 {
		msgTime = time.Unix(int64(sourceMsg.Message.Date), 0)
	}

	rawText := extractMessageContent(sourceMsg)
	if strings.TrimSpace(rawText) == "" {
		if groupCaption, err := infos.getMediaGroupCaption(ctx, sourceClient, sourceMsg); err != nil {
			debugf("消息组 caption 获取失败: cid=%d mid=%d err=%v", sourceMsg.ChatID(), sourceMsg.ID, err)
		} else if strings.TrimSpace(groupCaption) != "" {
			rawText = groupCaption
			debugf("消息组 caption 命中: cid=%d mid=%d caption=%q", sourceMsg.ChatID(), sourceMsg.ID, rawText)
		}
	}
	debugf("原始消息内容: cid=%d mid=%d caption=%q fileName=%q", sourceMsg.ChatID(), sourceMsg.ID, rawText, func() string {
		if sourceMsg.File != nil {
			return sourceMsg.File.Name
		}
		return ""
	}())

	channelName := strings.TrimSpace(sourceMsg.Channel.Title)
	if channelName == "" {
		channelName = strconv.FormatInt(sourceMsg.ChatID(), 10)
	}

	content := strings.TrimSpace(rawText)
	hasContent := content != ""
	content = sanitizeFileName(content)
	if maxLen := infos.captionMaxLength(); maxLen > 0 {
		truncated := truncateRunes(content, maxLen)
		if truncated != content {
			debugf("caption 超长，已截断: cid=%d mid=%d max=%d", sourceMsg.ChatID(), sourceMsg.ID, maxLen)
			content = truncated
		}
	}

	ext := determineFileExtension(sourceMsg)
	fileName := fmt.Sprintf("%d%s", sourceMsg.ID, ext)
	if hasContent && content != "" {
		fileName = fmt.Sprintf("%d - %s%s", sourceMsg.ID, content, ext)
	}

	info.MsgTime = msgTime
	info.ChannelName = channelName
	info.RawText = rawText
	info.Content = content
	info.HasContent = hasContent
	info.Ext = ext
	info.FileName = fileName
	info.FinalPath = buildMediaTargetPath(outputRoot, channelName, msgTime, fileName)
	return info, nil
}

func (infos *Infos) captionMaxLength() int {
	if infos == nil || infos.Conf == nil {
		return 100
	}
	if infos.Conf.Download.MaxCaptionLength <= 0 {
		return 100
	}
	return infos.Conf.Download.MaxCaptionLength
}

func truncateRunes(src string, maxLen int) string {
	if maxLen <= 0 {
		return src
	}
	runes := []rune(src)
	if len(runes) <= maxLen {
		return src
	}
	return string(runes[:maxLen])
}

// ensureExistingMediaTarget 统一处理“本地已存在 / rclone 已存在 / 本地存在时执行 rclone”的逻辑。
// 返回 handled=true 表示该目标已经被处理完成，调用方无需继续执行下载或发送流程。
func (infos *Infos) ensureExistingMediaTarget(ctx context.Context, outputRoot, finalPath string) (handled bool, err error) {
	if infos == nil {
		return false, nil
	}

	localExists, remoteExists, existsErr := infos.checkExistingLocalOrRemote(ctx, outputRoot, finalPath)
	if existsErr != nil {
		return false, existsErr
	}

	if remoteExists {
		log.Printf("rclone远程存在，跳过 path=%s", finalPath)
		sleepMS := 100
		if infos.Conf != nil && infos.Conf.Download.Rclone.RemoteExistsSleepMS > 0 {
			sleepMS = infos.Conf.Download.Rclone.RemoteExistsSleepMS
		}
		debugf("休眠 防止频繁被限制: sleep=%dms", sleepMS)
		time.Sleep(time.Duration(sleepMS) * time.Millisecond)
		return true, nil
	}

	if localExists {
		if infos.Conf != nil && infos.Conf.Download.Rclone.Enabled {
			remotePath, rcloneErr := infos.rcloneRemotePath(outputRoot, finalPath)
			if rcloneErr != nil {
				return true, rcloneErr
			}
			mode := infos.rcloneTransferMode()
			log.Printf("本地存在，执行 rclone %s path=%s", mode, finalPath)
			if rcloneErr := infos.rcloneTransferFile(ctx, finalPath, remotePath, mode); rcloneErr != nil {
				return true, rcloneErr
			}
			log.Printf("rclone %s 完成: %s", mode, finalPath)
			return true, nil
		}
		log.Printf("本地存在，跳过 path=%s", finalPath)
		return true, nil
	}

	return false, nil
}
