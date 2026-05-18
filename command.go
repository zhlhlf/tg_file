package main

import (
	"fmt"
	"html"
	"log"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/amarnathcjd/gogram/telegram"
)

func handleRelayInboxCapture(m *telegram.NewMessage) error {
	if m == nil || infos == nil {
		return nil
	}
	if !m.IsMedia() || m.Media() == nil || m.File == nil {
		if m != nil && infos != nil && infos.Conf != nil && infos.Conf.Debug {
			debugf("RelayInbox 忽略非媒体消息: client=%p mid=%d senderID=%d chatID=%d isMedia=%v fileNil=%v mediaType=%T", m.Client, m.ID, m.SenderID(), m.ChatID(), m.IsMedia(), m.File == nil, m.Media())
		}
		return nil
	}
	senderID := m.SenderID()
	if senderID == 0 || !infos.isInternalUserBot(senderID) {
		return nil
	}

	botID := int64(0)
	infos.Mutex.RLock()
	for idx, c := range infos.RelayBotClients {
		if c == m.Client && idx < len(infos.RelayBotIDs) {
			botID = infos.RelayBotIDs[idx]
			break
		}
	}
	infos.Mutex.RUnlock()
	if botID == 0 {
		if infos != nil && infos.Conf != nil && infos.Conf.Debug {
			caption := strings.TrimSpace(extractMessageContent(*m))
			debugf("RelayInbox 未识别到 Bot 实例: client=%p mid=%d senderID=%d chatID=%d caption=%q", m.Client, m.ID, senderID, m.ChatID(), caption)
		}
		return nil
	}
	infos.cacheRelayInboxMedia(botID, senderID, *m)
	return nil
}

// handleBotCommand 是 Bot 的总消息分发入口，处理所有管理指令
func handleBotCommand(m *telegram.NewMessage) error {
	if _, ok := infos.BotIDs[m.Sender.ID]; ok {
		return nil
	}

	text := strings.TrimSpace(m.Text())

	// 以 / 开头的命令消息，1分钟后自动删除
	if strings.HasPrefix(text, "/") {
		go func() {
			time.Sleep(60 * time.Second)
			if _, err := m.Delete(); err != nil {
				log.Printf("删除命令消息失败: %+v", err)
			}
		}()
	}

	if m.Channel == nil {
		ownerID := infos.currentAdminUserID()
		switch {
		case strings.HasPrefix(text, "/start"):
			if !infos.isAllowedBotSender(m.SenderID()) {
				sendMS(m, "你没有使用此机器人的权限", nil, 60)
				return nil
			}

			var src string
			if ownerID != 0 && m.SenderID() == ownerID {
				switch infos.Status.Load() {
				case 0:
					src = "userBot 未登录, 仅使用 Bot 或发送 /phone 手机号登录 userBot"
				case 1:
					src = "正在等待验证码, 请发送 /code 验证码"
				case 2:
					src = "正在等待密码, 请发送 /pass 密码"
				case 3:
					src = "userBot 已登录"
				}
			} else {
				src = "仅限内部使用, 请保管好你的HASH密码与UID"
			}
			sendMS(m, src, nil)
			return nil
		case strings.HasPrefix(text, "/allow"):
			if !infos.isAdmin(m.SenderID()) {
				sendMS(m, "你没有使用此命令的权限", nil, 60)
				return nil
			}
			whiteID, err := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(text, "/allow")), 10, 64)
			if err != nil {
				sendMS(m, fmt.Sprintf("添加白名单失败: %+v", err), nil, 60)
				return nil
			}

			if whiteID != 0 {
				if slices.Contains(infos.Conf.WhiteIDs, whiteID) {
					sendMS(m, fmt.Sprintf("白名单中已存在: %d", whiteID), nil, 60)
					return nil
				}

				infos.Mutex.Lock()
				value := ID{
					IsWhite: true,
				}
				infos.IDs[whiteID] = value
				infos.Conf.WhiteIDs = append(infos.Conf.WhiteIDs, whiteID)
				if err := saveConf(infos.Conf, infos.FilesPath); err != nil {
					log.Printf("保存配置文件失败: %+v", err)
				}
				infos.Mutex.Unlock()
				sendMS(m, fmt.Sprintf("添加白名单成功: %d", whiteID), nil, 60)
			}
			return nil
		case strings.HasPrefix(text, "/disallow"):
			if !infos.isAdmin(m.SenderID()) {
				sendMS(m, "你没有使用此命令的权限", nil, 60)
				return nil
			}
			whiteID, err := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(text, "/disallow")), 10, 64)
			if err != nil {
				sendMS(m, fmt.Sprintf("移除白名单失败: %+v", err), nil, 60)
				return nil
			}

			if whiteID != 0 {
				if slices.Contains(infos.Conf.WhiteIDs, whiteID) {
					infos.Mutex.Lock()
					delete(infos.IDs, whiteID)
					infos.Conf.WhiteIDs = slices.DeleteFunc(infos.Conf.WhiteIDs, func(num int64) bool {
						return num == whiteID
					})
					if err := saveConf(infos.Conf, infos.FilesPath); err != nil {
						log.Printf("保存配置文件失败: %+v", err)
					}
					infos.Mutex.Unlock()
					sendMS(m, fmt.Sprintf("移除白名单成功: %d", whiteID), nil, 60)
					return nil
				}
				sendMS(m, fmt.Sprintf("用户 %d 不在白名单中", whiteID), nil, 60)
			}
			return nil
		case strings.HasPrefix(text, "/qr"):
			if ownerID == 0 || m.SenderID() != ownerID {
				sendMS(m, "你没有使用此命令的权限", nil, 60)
				return nil
			}
			if err := infos.startUserBotQR(); err != nil {
				sendMS(m, fmt.Sprintf("启动 QR 登录失败: %+v", err), nil, 60)
			}
			return nil
		case strings.HasPrefix(text, "/makebots"):
			if !infos.isAdmin(m.SenderID()) {
				sendMS(m, "你没有使用此命令的权限", nil, 60)
				return nil
			}
			sendMS(m, "开始使用第一个 UserBot 创建 5 个机器人，token 将写回 config.yaml", nil, 60)
			tokens, err := infos.createBotsWithFirstUserBot(5)
			if err != nil {
				sendMS(m, fmt.Sprintf("创建机器人失败: %+v", err), nil, 120)
				return nil
			}
			sendMS(m, fmt.Sprintf("创建完成，共 %d 个，token 已写入 config.yaml", len(tokens)), nil, 120)
			return nil
		case strings.HasPrefix(text, "/phone"):
			if ownerID == 0 || m.SenderID() != ownerID {
				sendMS(m, "你没有使用此命令的权限", nil, 60)
				return nil
			}
			content := strings.TrimSpace(strings.TrimPrefix(text, "/phone"))
			if content == "" {
				sendMS(m, "手机不能为空", nil, 60)
				return nil
			}
			if !strings.HasPrefix(content, "+") {
				content = "+" + content
			}
			if err := infos.startUserBot(content); err != nil {
				sendMS(m, fmt.Sprintf("启动 UserBot 失败: %+v", err), nil, 60)
			}
			return nil
		case strings.HasPrefix(text, "/code"):
			if ownerID == 0 || m.SenderID() != ownerID {
				sendMS(m, "你没有使用此命令的权限", nil, 60)
				return nil
			}
			code := strings.TrimSpace(strings.TrimPrefix(text, "/code"))
			if code == "" {
				sendMS(m, "验证码不能为空", nil, 60)
				return nil
			}
			if err := infos.submitCode(code); err != nil {
				sendMS(m, fmt.Sprintf("提交验证码失败: %+v", err), nil, 60)
				return nil
			}
			sendMS(m, "提交验证码成功", nil, 60)
			return nil
		case strings.HasPrefix(text, "/pass") && !strings.HasPrefix(text, "/password"):
			if ownerID == 0 || m.SenderID() != ownerID {
				sendMS(m, "你没有使用此命令的权限", nil, 60)
				return nil
			}
			pass := strings.TrimSpace(strings.TrimPrefix(text, "/pass"))
			if pass == "" {
				sendMS(m, "2FA密码不能为空", nil, 60)
				return nil
			}
			if err := infos.submitPass(pass); err != nil {
				sendMS(m, fmt.Sprintf("提交2FA密码失败: %+v", err), nil, 60)
				return nil
			}
			sendMS(m, "提交2FA密码成功", nil, 60)
			return nil
		case strings.HasPrefix(text, "/dc"):
			if !infos.isAdmin(m.SenderID()) {
				sendMS(m, "你没有使用此命令的权限", nil, 60)
				return nil
			}
			content := strings.TrimSpace(strings.TrimPrefix(text, "/dc"))
			if content == "" {
				if infos.Conf.DC != 0 {
					sendMS(m, fmt.Sprintf("当前DC: %d", infos.Conf.DC), nil, 60)
				} else {
					sendMS(m, "当前未手动指定DC", nil, 60)
				}
				return nil
			}
			value, err := strconv.Atoi(content)
			if err != nil {
				sendMS(m, fmt.Sprintf("DC格式错误: %+v", err), nil, 60)
				return nil
			}
			if value < 1 || value > 5 {
				sendMS(m, "DC必须在1-5之间", nil, 60)
				return nil
			}
			infos.Mutex.Lock()
			infos.Conf.DC = value
			if err := saveConf(infos.Conf, infos.FilesPath); err != nil {
				log.Printf("保存配置文件失败: %+v", err)
			}
			infos.Mutex.Unlock()
			sendMS(m, fmt.Sprintf("DC已设置为: %d, 重启后生效", value), nil, 60)
			return nil
		case strings.HasPrefix(text, "/proxy"):
			if !infos.isAdmin(m.SenderID()) {
				sendMS(m, "你没有使用此命令的权限", nil, 60)
				return nil
			}
			content := strings.TrimSpace(strings.TrimPrefix(text, "/proxy"))
			if content == "" {
				if infos.Conf.Proxy == "" {
					sendMS(m, "当前未设置代理", nil, 60)
					return nil
				} else {
					sendMS(m, fmt.Sprintf("当前代理: %s", infos.Conf.Proxy), nil, 60)
					return nil
				}
			}
			if content == "off" {
				content = ""
			}
			if _, err := telegram.ProxyFromURL(content); err != nil {
				sendMS(m, "代理地址格式错误", nil, 60)
				return nil
			}
			infos.Mutex.Lock()
			infos.Conf.Proxy = content
			if err := saveConf(infos.Conf, infos.FilesPath); err != nil {
				log.Printf("保存配置文件失败: %+v", err)
			}
			infos.Mutex.Unlock()
			sendMS(m, fmt.Sprintf("代理已设置为: %s", content), nil, 60)
			return nil
		case strings.HasPrefix(text, "/workers"):
			if !infos.isAdmin(m.SenderID()) {
				sendMS(m, "你没有使用此命令的权限", nil, 60)
				return nil
			}
			content := strings.TrimSpace(strings.TrimPrefix(text, "/workers"))
			if content == "" {
				sendMS(m, fmt.Sprintf("当前并发数: %d", infos.Conf.Workers), nil, 60)
				return nil
			}
			num, err := strconv.Atoi(content)
			if err != nil {
				sendMS(m, "并发数必须为数字", nil, 60)
				return nil
			}
			if num <= 0 {
				sendMS(m, "并发数必须大于 0", nil, 60)
				return nil
			}
			infos.Mutex.Lock()
			infos.Conf.Workers = num
			if err := saveConf(infos.Conf, infos.FilesPath); err != nil {
				log.Printf("保存配置文件失败: %+v", err)
			}
			infos.Mutex.Unlock()
			src := fmt.Sprintf("并发数已设置为: %d", num)
			if num > 4 {
				src += ", 当前并发数较大, 容易引起下载失败甚至封号, 请谨慎设置"
			}
			sendMS(m, src, nil, 60)
			return nil
		case strings.HasPrefix(text, "/list"):
			if !infos.isAdmin(m.SenderID()) {
				sendMS(m, "你没有使用此命令的权限", nil, 60)
				return nil
			}
			content := strings.TrimSpace(strings.TrimPrefix(text, "/list"))
			if content != "" && content != "ids" {
				sendMS(m, "类别错误", nil, 60)
				return nil
			}
			var values strings.Builder
			count := len(infos.Conf.WhiteIDs)
			if count == 0 {
				sendMS(m, "⚠️ <b>白名单目前为空</b>", nil, 60)
				return nil
			}
			values.WriteString(fmt.Sprintf("🛡️ <b>白名单 ID 列表</b> (共 %d 个)\n", count))
			values.WriteString("━━━━━━━━━━━━━━━\n")
			for _, whiteID := range infos.Conf.WhiteIDs {
				values.WriteString(fmt.Sprintf("• <code>%d</code>\n", whiteID))
			}
			sendMS(m, values.String(), nil, 60)
			return nil
		case strings.HasPrefix(text, "/info"):
			if !infos.isAdmin(m.SenderID()) {
				sendMS(m, "你没有使用此命令的权限", nil, 60)
				return nil
			}

			num := 10
			content := strings.TrimSpace(strings.TrimPrefix(text, "/info"))
			if content != "" {
				src, value := extractContent(content)
				if value != nil {
					num = *value
				}
				content = src
			}

			if infos.FilePath == "" {
				sendMS(m, "暂未开启日志记录", nil, 60)
				return nil
			}

			lines, err := readLastLines(infos.FilePath, content, num)
			if err != nil {
				sendMS(m, fmt.Sprintf("读取日志失败: %+v", err), nil, 60)
				return nil
			}

			if len(lines) == 0 {
				sendMS(m, "暂无日志内容", nil, 60)
				return nil
			}

			const maxCount = 4000
			var values strings.Builder
			values.WriteString(fmt.Sprintf("<b>📜 系统日志 (最后 %d 行)</b>\n\n", len(lines)))
			values.WriteString("<pre>")

			for _, line := range lines {
				line = html.EscapeString(line) + "\n"
				if values.Len()+len(line)+len("</pre>") > maxCount {
					values.WriteString("</pre>")
					sendMS(m, values.String(), nil)
					values.Reset()
					values.WriteString("<pre>")
				}
				values.WriteString(line)
			}

			if values.Len() > len("<pre>") {
				values.WriteString("</pre>")
				sendMS(m, values.String(), nil)
			}
			return nil
		default:
			if !infos.isAllowedBotSender(m.SenderID()) && m.SenderID() != 0 {
				sendMS(m, "你没有使用此机器人的权限", nil, 60)
				return nil
			}
			return nil
		}
	}
	return nil
}

// sendMS 统一发送消息，支持回复或主动推送给管理员，可设置自动删除延时
func sendMS(m *telegram.NewMessage, src any, params *telegram.SendOptions, wait ...int) {
	switch {
	case m != nil:
		ms, err := m.Reply(src, params)
		if err != nil {
			log.Printf("发送消息失败: %+v", err)
		}
		if len(wait) > 0 && wait[0] > 0 {
			go func() {
				time.Sleep(time.Duration(wait[0]) * time.Second)
				if _, err = ms.Delete(); err != nil {
					log.Printf("删除消息失败: %+v", err)
				}
			}()
		}
		return
	case infos.BotClient != nil:
		targetID := infos.notificationTargetID()
		if targetID == 0 {
			log.Printf("跳过主动发送消息: 无可用通知目标, message=%v", src)
			return
		}
		ms, err := infos.BotClient.SendMessage(targetID, src, params)
		if err != nil {
			log.Printf("发送消息失败: %+v", err)
		}
		if len(wait) > 0 && wait[0] > 0 && ms != nil {
			go func() {
				time.Sleep(time.Duration(wait[0]) * time.Second)
				if _, err = ms.Delete(); err != nil {
					log.Printf("删除消息失败: %+v", err)
				}
			}()
		}
		return
	}
}
