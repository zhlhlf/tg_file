// use bots to relay media messages for downloading, to bypass some limitations of userbots and improve success rate

package main

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"github.com/amarnathcjd/gogram/telegram"
)

func (infos *Infos) prepareRelayBots() error {
	infos.RelayBotClients = nil
	infos.RelayBotLabels = nil
	infos.RelayBotIDs = nil
	infos.RelayBotTargets = nil
	availableBots := make([]string, 0, len(infos.BotClients))
	if len(infos.BotClients) == 0 {
		return fmt.Errorf("未配置任何 Bot")
	}
	for idx, client := range infos.BotClients {
		if client == nil {
			continue
		}
		me, err := client.GetMe()
		if err != nil {
			log.Printf("Bot[%d] 获取自身信息失败: %v", idx+1, err)
			continue
		}
		infos.RelayBotClients = append(infos.RelayBotClients, client)
		infos.RelayBotLabels = append(infos.RelayBotLabels, fmt.Sprintf("bot%d", idx+1))
		infos.RelayBotIDs = append(infos.RelayBotIDs, me.ID)
		target := ""
		if me.Username != "" {
			target = "@" + me.Username
		}
		infos.RelayBotTargets = append(infos.RelayBotTargets, target)
		availableBots = append(availableBots, me.Username)
	}
	if len(infos.RelayBotClients) == 0 {
		return fmt.Errorf("没有任何可用 Bot 用于分流下载")
	}
	log.Printf("可用bot列表(%d): [%s]", len(availableBots), strings.Join(availableBots, ","))
	return nil
}

func extractPeerID(peer any) (int64, error) {
	rValue := reflect.ValueOf(peer)
	if !rValue.IsValid() {
		return 0, fmt.Errorf("peer 无效")
	}
	if rValue.Kind() == reflect.Ptr {
		if rValue.IsNil() {
			return 0, fmt.Errorf("peer 为空")
		}
		rValue = rValue.Elem()
	}
	for _, fieldName := range []string{"ChannelID", "ChatID", "UserID", "ID"} {
		field := rValue.FieldByName(fieldName)
		if field.IsValid() && field.CanInt() {
			return field.Int(), nil
		}
	}
	return 0, fmt.Errorf("无法从 peer 中提取 ID: %T", peer)
}

func (infos *Infos) pickRelayBot(counter *uint64) (*telegram.Client, string, int64, string, error) {
	if len(infos.RelayBotClients) == 0 {
		return nil, "", 0, "", fmt.Errorf("没有可用的分流下载 Bot")
	}
	idx := int(atomic.AddUint64(counter, 1)-1) % len(infos.RelayBotClients)
	return infos.RelayBotClients[idx], infos.RelayBotLabels[idx], infos.RelayBotIDs[idx], infos.RelayBotTargets[idx], nil
}

func relayInboxKey(botID, senderID int64) string {
	return fmt.Sprintf("%d:%d", botID, senderID)
}

func relayCaptionKey(chatID int64, msgID int32) string {
	if chatID == 0 || msgID == 0 {
		return ""
	}
	return fmt.Sprintf("%d_%d", chatID, msgID)
}

func normalizeRelayInboxCaption(msg telegram.NewMessage) string {
	return strings.TrimSpace(extractMessageContent(msg))
}

func (infos *Infos) cacheRelayInboxMedia(botID, senderID int64, msg telegram.NewMessage) {
	if infos == nil || botID == 0 || senderID == 0 {
		return
	}
	if !msg.IsMedia() || msg.Media() == nil || msg.File == nil {
		return
	}
	receivedAt := time.Now().Unix()
	if msg.Message != nil && msg.Message.Date != 0 {
		receivedAt = int64(msg.Message.Date)
	}
	infos.Mutex.Lock()
	if infos.RelayInbox == nil {
		infos.RelayInbox = make(map[string][]RelayInboxRecord, 16)
	}
	key := relayInboxKey(botID, senderID)
	record := RelayInboxRecord{Msg: msg, ReceivedAt: receivedAt, Caption: normalizeRelayInboxCaption(msg)}
	records := infos.RelayInbox[key]
	records = append(records, record)
	const maxRelayInboxItems = 5
	if len(records) > maxRelayInboxItems {
		records = append([]RelayInboxRecord(nil), records[len(records)-maxRelayInboxItems:]...)
	}
	infos.RelayInbox[key] = records
	infos.Mutex.Unlock()
}

func (infos *Infos) getRelayInboxMedia(botID, senderID, minUnix int64, wantedCaption string) (telegram.NewMessage, bool) {
	if infos == nil || botID == 0 || senderID == 0 {
		return telegram.NewMessage{}, false
	}
	infos.Mutex.RLock()
	records, ok := infos.RelayInbox[relayInboxKey(botID, senderID)]
	infos.Mutex.RUnlock()
	if !ok {
		if infos != nil && infos.Conf != nil && infos.Conf.Debug {
			debugf("RelayInbox 未命中键: botID=%d senderID=%d wantedCaption=%q", botID, senderID, strings.TrimSpace(wantedCaption))
		}
		return telegram.NewMessage{}, false
	}
	wantedCaption = strings.TrimSpace(wantedCaption)
	for idx := len(records) - 1; idx >= 0; idx-- {
		rec := records[idx]
		if minUnix > 0 && rec.ReceivedAt < minUnix {
			if infos != nil && infos.Conf != nil && infos.Conf.Debug {
				debugf("RelayInbox 跳过旧消息: botID=%d senderID=%d mid=%d receivedAt=%d minUnix=%d caption=%q", botID, senderID, rec.Msg.ID, rec.ReceivedAt, minUnix, strings.TrimSpace(rec.Caption))
			}
			continue
		}
		if !rec.Msg.IsMedia() || rec.Msg.Media() == nil || rec.Msg.File == nil {
			if infos != nil && infos.Conf != nil && infos.Conf.Debug {
				debugf("RelayInbox 跳过无效媒体: botID=%d senderID=%d mid=%d isMedia=%v fileNil=%v mediaType=%T", botID, senderID, rec.Msg.ID, rec.Msg.IsMedia(), rec.Msg.File == nil, rec.Msg.Media())
			}
			continue
		}
		if wantedCaption != "" {
			recCaption := strings.TrimSpace(rec.Caption)
			if recCaption == "" {
				recCaption = normalizeRelayInboxCaption(rec.Msg)
			}
			if recCaption != wantedCaption {
				if infos != nil && infos.Conf != nil && infos.Conf.Debug {
					debugf("RelayInbox caption 不匹配: botID=%d senderID=%d mid=%d wanted=%q actual=%q", botID, senderID, rec.Msg.ID, wantedCaption, recCaption)
				}
				continue
			}
		}
		return rec.Msg, true
	}
	if infos != nil && infos.Conf != nil && infos.Conf.Debug {
		debugf("RelayInbox 有缓存但未匹配: botID=%d senderID=%d wantedCaption=%q cached=%d", botID, senderID, wantedCaption, len(records))
	}
	return telegram.NewMessage{}, false
}

func previewFirstBytes(v any, limit int) (text string, hex string, total int) {
	if limit <= 0 {
		limit = 200
	}
	raw := []byte(fmt.Sprintf("%#v", v))
	total = len(raw)
	if len(raw) > limit {
		raw = raw[:limit]
	}
	return string(raw), fmt.Sprintf("%x", raw), total
}

func formatRate(bytesPerSec float64) string {
	if bytesPerSec < 0 {
		bytesPerSec = 0
	}
	units := []string{"B", "KB", "MB", "GB", "TB"}
	idx := 0
	for bytesPerSec >= 1024 && idx < len(units)-1 {
		bytesPerSec /= 1024
		idx++
	}
	if idx == 0 {
		return fmt.Sprintf("%.0f%s", bytesPerSec, units[idx])
	}
	return fmt.Sprintf("%.2f%s", bytesPerSec, units[idx])
}

func (infos *Infos) withRelayForwardLock(fn func() error) error {
	if infos == nil {
		return fmt.Errorf("infos 为空")
	}
	if infos.RelayForwardSem == nil {
		return fn()
	}

	infos.RelayForwardSem <- struct{}{}
	defer func() {
		time.Sleep(150 * time.Millisecond)
		<-infos.RelayForwardSem
	}()

	return fn()
}

func (infos *Infos) ensureRelayBotAlive(ctx context.Context, relayBot *telegram.Client, relayLabel string) error {
	if relayBot == nil {
		return fmt.Errorf("relay bot 为空: bot=%s", relayLabel)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	latency, err := relayBot.Ping(pingCtx)
	if err == nil {
		debugf("回流 Bot 在线: bot=%s latency=%dms", relayLabel, latency.Milliseconds())
		return nil
	}

	log.Printf("回流 Bot Ping 失败，准备重连: bot=%s err=%v", relayLabel, err)
	if disconnectErr := relayBot.Disconnect(); disconnectErr != nil {
		log.Printf("回流 Bot 断开旧连接失败: bot=%s err=%v", relayLabel, disconnectErr)
	}
	if connectErr := relayBot.Connect(); connectErr != nil {
		log.Printf("回流 Bot 重连失败: bot=%s err=%v", relayLabel, connectErr)
		return connectErr
	}

	recheckCtx, recheckCancel := context.WithTimeout(ctx, 5*time.Second)
	defer recheckCancel()
	recheckLatency, recheckErr := relayBot.Ping(recheckCtx)
	if recheckErr != nil {
		log.Printf("回流 Bot 重连后 Ping 失败: bot=%s err=%v", relayLabel, recheckErr)
		return recheckErr
	}

	log.Printf("回流 Bot 已重连恢复: bot=%s latency=%dms", relayLabel, recheckLatency.Milliseconds())
	return nil
}

func (infos *Infos) ensureUserBotAlive(ctx context.Context, userClient *telegram.Client, userLabel string) error {
	if userClient == nil {
		return fmt.Errorf("user bot 为空: user=%s", userLabel)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	latency, err := userClient.Ping(pingCtx)
	if err == nil {
		debugf("UserBot 在线: user=%s latency=%dms", userLabel, latency.Milliseconds())
		return nil
	}

	log.Printf("UserBot Ping 失败，准备重连: user=%s err=%v", userLabel, err)
	if disconnectErr := userClient.Disconnect(); disconnectErr != nil {
		log.Printf("UserBot 断开旧连接失败: user=%s err=%v", userLabel, disconnectErr)
	}
	if connectErr := userClient.Connect(); connectErr != nil {
		log.Printf("UserBot 重连失败: user=%s err=%v", userLabel, connectErr)
		return connectErr
	}

	recheckCtx, recheckCancel := context.WithTimeout(ctx, 5*time.Second)
	defer recheckCancel()
	recheckLatency, recheckErr := userClient.Ping(recheckCtx)
	if recheckErr != nil {
		log.Printf("UserBot 重连后 Ping 失败: user=%s err=%v", userLabel, recheckErr)
		return recheckErr
	}

	log.Printf("UserBot 已重连恢复: user=%s latency=%dms", userLabel, recheckLatency.Milliseconds())
	return nil
}

func (infos *Infos) downloadMessageViaRelay(ctx context.Context, userClient *telegram.Client, outputRoot string, sourceMsg telegram.NewMessage, userAccount string, counter *uint64, cache *mediaResolveCache) error {
	relayBot, relayLabel, relayBotID, relayTarget, err := infos.pickRelayBot(counter)
	if err != nil {
		return err
	}
	if !sourceMsg.IsMedia() || sourceMsg.Media() == nil {
		return fmt.Errorf("源消息不包含可发送媒体: cid=%d mid=%d", sourceMsg.ChatID(), sourceMsg.ID)
	}

	refreshedMsg := sourceMsg
	refreshedMessages, refreshErr := userClient.GetMessages(sourceMsg.ChatID(), &telegram.SearchOption{IDs: []int32{sourceMsg.ID}})
	if refreshErr != nil {
		log.Printf("刷新源消息失败，尝试保活 UserBot: cid=%d mid=%d user=%s err=%v", sourceMsg.ChatID(), sourceMsg.ID, userAccount, refreshErr)
		if keepAliveErr := infos.ensureUserBotAlive(ctx, userClient, userAccount); keepAliveErr != nil {
			return fmt.Errorf("刷新源消息失败且 UserBot 保活失败: cid=%d mid=%d user=%s err=%v keepalive=%w", sourceMsg.ChatID(), sourceMsg.ID, userAccount, refreshErr, keepAliveErr)
		}
		refreshedMessages, refreshErr = userClient.GetMessages(sourceMsg.ChatID(), &telegram.SearchOption{IDs: []int32{sourceMsg.ID}})
		if refreshErr != nil {
			return fmt.Errorf("刷新源消息失败: cid=%d mid=%d err=%w", sourceMsg.ChatID(), sourceMsg.ID, refreshErr)
		}
	}
	if len(refreshedMessages) == 0 {
		return fmt.Errorf("刷新源消息为空: cid=%d mid=%d", sourceMsg.ChatID(), sourceMsg.ID)
	}
	refreshedMsg = refreshedMessages[0]
	if !refreshedMsg.IsMedia() || refreshedMsg.Media() == nil {
		return fmt.Errorf("刷新后的源消息不包含媒体: cid=%d mid=%d", refreshedMsg.ChatID(), refreshedMsg.ID)
	}

	targetInfo, err := infos.resolveMediaTarget(ctx, userClient, outputRoot, refreshedMsg, cache)
	if err != nil {
		return err
	}
	if infos.shouldSkipByFileName(targetInfo.FileName, targetInfo.FinalPath) {
		return nil
	}
	if handled, err := infos.ensureExistingMediaTarget(ctx, outputRoot, targetInfo.FinalPath); err != nil {
		return err
	} else if handled {
		return nil
	}

	forwardTarget := any(relayBotID)
	if relayTarget != "" {
		resolvedPeer, resolveErr := userClient.ResolvePeer(relayTarget)
		if resolveErr != nil {
			return fmt.Errorf("解析 Bot 私聊目标失败: bot=%s target=%s err=%w", relayLabel, relayTarget, resolveErr)
		}
		forwardTarget = resolvedPeer
	}

	captionKey := relayCaptionKey(refreshedMsg.ChatID(), refreshedMsg.ID)
	var relaySent *telegram.NewMessage
	err = infos.withRelayForwardLock(func() error {
		var sendErr error
		relaySent, sendErr = userClient.SendMedia(forwardTarget, refreshedMsg.Media(), &telegram.MediaOptions{Caption: captionKey})
		if sendErr != nil {
			return fmt.Errorf("发送媒体到 Bot 私聊失败: bot=%s target=%v err=%w", relayLabel, forwardTarget, sendErr)
		}
		if relaySent == nil || relaySent.Message == nil {
			return fmt.Errorf("发送媒体到 Bot 私聊后返回消息为空: bot=%s target=%v", relayLabel, forwardTarget)
		}
		return nil
	})
	if err != nil {
		return err
	}
	debugf("Send返回媒体状态: bot=%s mid=%d isMedia=%v fileNil=%v mediaType=%T", relayLabel, relaySent.ID, relaySent.IsMedia(), relaySent.File == nil, relaySent.Media())

	senderID := int64(0)
	mappedID := int64(0)
	if me, meErr := userClient.GetMe(); meErr == nil && me != nil {
		senderID = me.ID
	}
	if infos != nil && infos.UserClientIDs != nil {
		if mid := infos.UserClientIDs[userAccount]; mid != 0 {
			mappedID = mid
			if senderID == 0 || senderID == relayBotID {
				senderID = mid
			}
		}
	}
	if relaySent.Message != nil && relaySent.Message.FromID != nil {
		if uid, uidErr := extractPeerID(relaySent.Message.FromID); uidErr == nil && uid != 0 {
			if senderID == 0 || senderID == relayBotID {
				senderID = uid
			}
		}
	}
	if senderID == 0 {
		return fmt.Errorf("无法确定 Bot 端会话 peer: bot=%s mid=%d", relayLabel, relaySent.ID)
	}
	if senderID == relayBotID {
		return fmt.Errorf("检测到异常会话ID（与 Bot 自身相同）: bot=%s senderID=%d user=%s mid=%d", relayLabel, senderID, userAccount, relaySent.ID)
	}
	if err := infos.ensureRelayBotAlive(ctx, relayBot, relayLabel); err != nil {
		return fmt.Errorf("回流等待前 Bot 不在线: bot=%s err=%w", relayLabel, err)
	}
	debugf("Bot 开始等待回流媒体: bot=%s user=%s senderID=%d mappedID=%d botID=%d mid=%d caption=%s", relayLabel, userAccount, senderID, mappedID, relayBotID, relaySent.ID, captionKey)
	for i := 1; i <= 6; i++ {
		if cachedMsg, ok := infos.getRelayInboxMedia(relayBotID, senderID, 0, captionKey); ok {
			debugf("Bot 命中回流媒体: bot=%s senderID=%d cachedMid=%d attempt=%d caption=%s", relayLabel, senderID, cachedMsg.ID, i, captionKey)
			cachedMsg.Client = relayBot
			return infos.downloadMessageToFile(ctx, userClient, relayBot, outputRoot, refreshedMsg, cachedMsg, userAccount+"->"+relayLabel, cache)
		}
		if i == 3 || i == 6 {
			if err := infos.ensureRelayBotAlive(ctx, relayBot, relayLabel); err != nil {
				return fmt.Errorf("回流等待期间 Bot 失联: bot=%s attempt=%d err=%w", relayLabel, i, err)
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err := infos.ensureRelayBotAlive(ctx, relayBot, relayLabel); err != nil {
		return fmt.Errorf("回流等待超时前检测到 Bot 失联: bot=%s err=%w", relayLabel, err)
	}

	return fmt.Errorf("Bot 监听缓存中未拿到媒体，放弃本次并由上层重试: bot=%s mid=%d", relayLabel, relaySent.ID)
}
