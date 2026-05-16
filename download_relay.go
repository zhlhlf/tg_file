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
	log.Printf("可用bot列表: [%s]", strings.Join(availableBots, ","))
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
		infos.RelayInbox = make(map[string]RelayInboxRecord, 16)
	}
	infos.RelayInbox[relayInboxKey(botID, senderID)] = RelayInboxRecord{Msg: msg, ReceivedAt: receivedAt}
	infos.Mutex.Unlock()
}

func (infos *Infos) getRelayInboxMedia(botID, senderID, minUnix int64) (telegram.NewMessage, bool) {
	if infos == nil || botID == 0 || senderID == 0 {
		return telegram.NewMessage{}, false
	}
	infos.Mutex.RLock()
	rec, ok := infos.RelayInbox[relayInboxKey(botID, senderID)]
	infos.Mutex.RUnlock()
	if !ok {
		return telegram.NewMessage{}, false
	}
	if minUnix > 0 && rec.ReceivedAt < minUnix {
		return telegram.NewMessage{}, false
	}
	if !rec.Msg.IsMedia() || rec.Msg.Media() == nil || rec.Msg.File == nil {
		return telegram.NewMessage{}, false
	}
	return rec.Msg, true
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

func (infos *Infos) downloadMessageViaRelay(ctx context.Context, userClient *telegram.Client, outputRoot string, sourceMsg telegram.NewMessage, userAccount string, counter *uint64) error {
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
		return fmt.Errorf("刷新源消息失败: cid=%d mid=%d err=%w", sourceMsg.ChatID(), sourceMsg.ID, refreshErr)
	}
	if len(refreshedMessages) == 0 {
		return fmt.Errorf("刷新源消息为空: cid=%d mid=%d", sourceMsg.ChatID(), sourceMsg.ID)
	}
	refreshedMsg = refreshedMessages[0]
	if !refreshedMsg.IsMedia() || refreshedMsg.Media() == nil {
		return fmt.Errorf("刷新后的源消息不包含媒体: cid=%d mid=%d", refreshedMsg.ChatID(), refreshedMsg.ID)
	}

	targetInfo, err := infos.resolveMediaTarget(ctx, userClient, outputRoot, refreshedMsg)
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

	relaySent, err := userClient.SendMedia(forwardTarget, refreshedMsg.Media(), &telegram.MediaOptions{Caption: extractMessageContent(refreshedMsg)})
	if err != nil {
		return fmt.Errorf("发送媒体到 Bot 私聊失败: bot=%s target=%v err=%w", relayLabel, forwardTarget, err)
	}
	if relaySent == nil || relaySent.Message == nil {
		return fmt.Errorf("发送媒体到 Bot 私聊后返回消息为空: bot=%s target=%v", relayLabel, forwardTarget)
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
	botPeer := &telegram.PeerUser{UserID: senderID}
	debugf("Bot 拉取消息使用会话: bot=%s user=%s senderID=%d mappedID=%d botID=%d mid=%d", relayLabel, userAccount, senderID, mappedID, relayBotID, relaySent.ID)
	approxSendUnix := time.Now().Unix()
	if relaySent.Message != nil && relaySent.Message.Date != 0 {
		approxSendUnix = int64(relaySent.Message.Date)
	}

	for i := 1; i <= 6; i++ {
		if cachedMsg, ok := infos.getRelayInboxMedia(relayBotID, senderID, approxSendUnix-5); ok {
			debugf("命中 Bot 入站媒体缓存: bot=%s senderID=%d mid=%d attempt=%d", relayLabel, senderID, cachedMsg.ID, i)
			cachedMsg.Client = relayBot
			return infos.downloadMessageToFile(ctx, userClient, relayBot, outputRoot, refreshedMsg, cachedMsg, userAccount+"->"+relayLabel)
		}

		botMsgs, berr := relayBot.GetMessages(botPeer, &telegram.SearchOption{IDs: []int32{relaySent.ID}})
		if berr == nil && len(botMsgs) > 0 {
			debugf("Bot按ID消息状态: bot=%s mid=%d attempt=%d isMedia=%v fileNil=%v mediaType=%T", relayLabel, botMsgs[0].ID, i, botMsgs[0].IsMedia(), botMsgs[0].File == nil, botMsgs[0].Media())
			if botMsgs[0].IsMedia() && botMsgs[0].Media() != nil && botMsgs[0].File != nil {
				debugf("从 Bot 端获取到媒体引用（按ID）: bot=%s mid=%d attempt=%d", relayLabel, botMsgs[0].ID, i)
				botMsg := botMsgs[0]
				botMsg.Client = relayBot
				return infos.downloadMessageToFile(ctx, userClient, relayBot, outputRoot, refreshedMsg, botMsg, userAccount+"->"+relayLabel)
			}
			mediaType := "<nil>"
			if botMsgs[0].Message != nil && botMsgs[0].Message.Media != nil {
				mediaType = fmt.Sprintf("%T", botMsgs[0].Message.Media)
			}
			debugf("Bot 端拉取消息但未包含媒体: bot=%s mid=%d attempt=%d mediaType=%s", relayLabel, relaySent.ID, i, mediaType)
		} else if berr != nil {
			debugf("从 Bot 端按ID拉取消息失败: bot=%s mid=%d attempt=%d err=%v", relayLabel, relaySent.ID, i, berr)
			if strings.Contains(strings.ToLower(berr.Error()), "missing from cache") {
				_, _ = relayBot.GetDialogs(&telegram.DialogOptions{Limit: 50})
				debugf("Bot 对话缓存预热完成: bot=%s mid=%d attempt=%d", relayLabel, relaySent.ID, i)
			}
		}

		recentMsgs, rerr := relayBot.GetMessages(botPeer, &telegram.SearchOption{Limit: 50})
		if rerr == nil && len(recentMsgs) > 0 {
			var candidate *telegram.NewMessage
			for idx := range recentMsgs {
				m := recentMsgs[idx]
				if m.ID == relaySent.ID && m.IsMedia() && m.Media() != nil && m.File != nil {
					candidate = &m
					break
				}
			}
			if candidate == nil {
				for idx := range recentMsgs {
					m := recentMsgs[idx]
					if !m.IsMedia() || m.Media() == nil || m.File == nil {
						continue
					}
					if senderID != 0 && m.SenderID() != 0 && m.SenderID() != senderID {
						continue
					}
					if m.Message != nil && m.Message.Date != 0 {
						delta := int64(m.Message.Date) - approxSendUnix
						if delta < 0 {
							delta = -delta
						}
						if delta > 180 {
							continue
						}
					}
					if m.ID >= relaySent.ID-20 && m.ID <= relaySent.ID+20 {
						candidate = &m
						break
					}
				}
			}
			if candidate == nil {
				for idx := range recentMsgs {
					m := recentMsgs[idx]
					if !m.IsMedia() || m.Media() == nil || m.File == nil {
						continue
					}
					if senderID != 0 && m.SenderID() != 0 && m.SenderID() != senderID {
						continue
					}
					candidate = &m
					break
				}
			}
			if candidate != nil {
				debugf("从 Bot 最近消息窗口匹配到媒体: bot=%s wantedMid=%d gotMid=%d sender=%d attempt=%d", relayLabel, relaySent.ID, candidate.ID, candidate.SenderID(), i)
				candidate.Client = relayBot
				return infos.downloadMessageToFile(ctx, userClient, relayBot, outputRoot, refreshedMsg, *candidate, userAccount+"->"+relayLabel)
			}
			debugf("Bot 最近消息窗口未匹配到媒体: bot=%s wantedMid=%d attempt=%d count=%d", relayLabel, relaySent.ID, i, len(recentMsgs))
		}
		time.Sleep(500 * time.Millisecond)
	}

	return fmt.Errorf("Bot 端消息未稳定为媒体，放弃本次并由上层重试: bot=%s mid=%d", relayLabel, relaySent.ID)
}
