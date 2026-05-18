package main

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
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

type mediaResolveCache struct {
	mu                sync.RWMutex
	messages          map[int32]telegram.NewMessage
	groupCaptionByID  map[int64]string
}

func newMediaResolveCache(messages []telegram.NewMessage) *mediaResolveCache {
	cache := &mediaResolveCache{
		messages:         make(map[int32]telegram.NewMessage, len(messages)),
		groupCaptionByID: make(map[int64]string),
	}
	for _, msg := range messages {
		cache.storeMessage(msg)
	}
	return cache
}

func (c *mediaResolveCache) storeMessage(msg telegram.NewMessage) {
	if c == nil || msg.ID == 0 {
		return
	}
	c.mu.Lock()
	c.messages[msg.ID] = msg
	c.mu.Unlock()
}

func (c *mediaResolveCache) findCaptionByGroupedID(groupedID int64) string {
	if c == nil || groupedID == 0 {
		return ""
	}
	c.mu.RLock()
	if caption := strings.TrimSpace(c.groupCaptionByID[groupedID]); caption != "" {
		c.mu.RUnlock()
		return caption
	}
	for _, msg := range c.messages {
		if msg.Message == nil || msg.Message.GroupedID != groupedID {
			continue
		}
		caption := strings.TrimSpace(extractMessageContent(msg))
		if caption != "" {
			c.mu.RUnlock()
			c.storeGroupCaption(groupedID, caption)
			return caption
		}
		}
	c.mu.RUnlock()
	return ""
}

func (c *mediaResolveCache) storeGroupCaption(groupedID int64, caption string) {
	if c == nil || groupedID == 0 {
		return
	}
	caption = strings.TrimSpace(caption)
	if caption == "" {
		return
	}
	c.mu.Lock()
	c.groupCaptionByID[groupedID] = caption
	c.mu.Unlock()
}

func buildMediaTargetPath(outputRoot, channelName string, msgTime time.Time, fileName string) string {
	channelName = sanitizeFileName(strings.TrimSpace(channelName))
	return filepath.Join(outputRoot, channelName, fmt.Sprintf("%04d_%02d", msgTime.Year(), msgTime.Month()), fileName)
}

func (infos *Infos) resolveMediaTarget(ctx context.Context, sourceClient *telegram.Client, outputRoot string, sourceMsg telegram.NewMessage, cache *mediaResolveCache) (mediaTargetInfo, error) {
	info := mediaTargetInfo{}

	msgTime := time.Now()
	if sourceMsg.Message != nil && sourceMsg.Message.Date != 0 {
		msgTime = time.Unix(int64(sourceMsg.Message.Date), 0)
	}

	rawText := extractMessageContent(sourceMsg)
	captionFromCache := false
	if strings.TrimSpace(rawText) == "" {
		if groupCaption, fromCache, err := infos.getMediaGroupCaption(ctx, sourceClient, sourceMsg, cache); err != nil {
			debugf("消息组 caption 获取失败: cid=%d mid=%d err=%v", sourceMsg.ChatID(), sourceMsg.ID, err)
		} else if strings.TrimSpace(groupCaption) != "" {
			rawText = groupCaption
			captionFromCache = fromCache
			debugf("消息组 caption 命中: cid=%d mid=%d fromCache=%t caption=%q", sourceMsg.ChatID(), sourceMsg.ID, captionFromCache, rawText)
		}
	}

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
		return 90
	}
	if infos.Conf.Download.MaxCaptionLength <= 0 {
		return 90
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

func (infos *Infos) getMediaGroupCaption(ctx context.Context, client *telegram.Client, msg telegram.NewMessage, cache *mediaResolveCache) (string, bool, error) {
	if client == nil || msg.Message == nil || msg.Message.GroupedID == 0 {
		return "", false, nil
	}
	if caption := cache.findCaptionByGroupedID(msg.Message.GroupedID); caption != "" {
		return caption, true, nil
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
		return "", false, nil
	}

	ms, err := client.GetMessages(msg.ChatID(), &telegram.SearchOption{IDs: ids})
	if err != nil {
		return "", false, err
	}
	for _, groupMsg := range ms {
		cache.storeMessage(groupMsg)
		if groupMsg.Message == nil || groupMsg.Message.GroupedID != msg.Message.GroupedID {
			continue
		}
		caption := strings.TrimSpace(extractMessageContent(groupMsg))
		if caption != "" {
			cache.storeGroupCaption(msg.Message.GroupedID, caption)
			return caption, false, nil
		}
	}
	return "", false, nil
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
	src = strings.ReplaceAll(src, "/", "_")
	src = strings.ReplaceAll(src, "\\", "_")
	src = strings.ReplaceAll(src, ":", "_")
	src = strings.ReplaceAll(src, "*", "_")
	src = strings.ReplaceAll(src, "?", "_")
	src = strings.ReplaceAll(src, "\"", "_")
	src = strings.ReplaceAll(src, "<", "_")
	src = strings.ReplaceAll(src, ">", "_")
	src = strings.ReplaceAll(src, "|", "_")
	src = strings.ReplaceAll(src, "\n", "_")
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
