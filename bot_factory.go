package main

import (
	"crypto/rand"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/amarnathcjd/gogram/telegram"
)

var botTokenPattern = regexp.MustCompile(`\b\d+:[A-Za-z0-9_-]{30,}\b`)
var botFatherTooManyAttemptsPattern = regexp.MustCompile(`(?i)too many attempts\. please try again in (\d+) seconds`)

type botFatherStep struct {
	containsAny []string
	timeout     time.Duration
}

type collectBotTokensResult struct {
	name   string
	tokens []string
	err    error
}

func (infos *Infos) createBotsWithFirstUserBot(count int) ([]string, error) {
	if infos == nil || infos.Conf == nil {
		return nil, fmt.Errorf("配置未初始化")
	}
	if count <= 0 {
		count = 5
	}

	_, client, err := infos.firstConfiguredUserClient()
	if err != nil {
		return nil, err
	}
	me, err := client.GetMe()
	if err != nil {
		return nil, fmt.Errorf("获取首个 UserBot 信息失败: %w", err)
	}
	peer, err := client.ResolvePeer("@BotFather")
	if err != nil {
		return nil, fmt.Errorf("解析 @BotFather 失败: %w", err)
	}

	lastSeen := infos.latestBotFatherReplyID(client, peer, me.ID)
	if _, err := infos.botFatherSendAndWait(client, peer, me.ID, "/cancel", &lastSeen, 12*time.Second); err != nil {
		debugf("BotFather /cancel 失败，继续尝试: %v", err)
	}

	createdTokens := make([]string, 0, count)
	for i := 0; i < count; i++ {
		if _, err := infos.botFatherSendAndWait(client, peer, me.ID, "/cancel", &lastSeen, 12*time.Second); err != nil {
			debugf("创建前重置 BotFather 状态失败，继续尝试: %v", err)
		}
		token, username, createErr := infos.createSingleBot(client, peer, me.ID, &lastSeen)
		if createErr != nil {
			return createdTokens, fmt.Errorf("创建第 %d 个 bot 失败: %w", i+1, createErr)
		}
		createdTokens = append(createdTokens, token)
		infos.appendBotToken(token)
		log.Printf("已创建 Bot: username=@%s", username)
		if i < count-1 {
			debugf("创建 Bot 后休眠: sleep=1m index=%d", i+1)
			time.Sleep(1 * time.Minute)
		}
	}

	if err := saveConf(infos.Conf, infos.FilesPath); err != nil {
		return createdTokens, fmt.Errorf("保存 botTokens 失败: %w", err)
	}
	debugf("批量创建 Bot 完成: count=%d", len(createdTokens))
	return createdTokens, nil
}

func (infos *Infos) collectAllBotTokensFromAllUsers() ([]string, error) {
	if infos == nil || infos.Conf == nil {
		return nil, fmt.Errorf("配置未初始化")
	}
	infos.Mutex.RLock()
	clients := make(map[string]*telegram.Client, len(infos.UserClients))
	for name, client := range infos.UserClients {
		clients[name] = client
	}
	infos.Mutex.RUnlock()
	if len(clients) == 0 {
		return nil, fmt.Errorf("未找到可用 UserBot 客户端")
	}

	results := make(chan collectBotTokensResult, len(clients))
	var wg sync.WaitGroup
	for name, client := range clients {
		if client == nil {
			continue
		}
		wg.Add(1)
		go func(name string, client *telegram.Client) {
			defer wg.Done()
			peer, err := client.ResolvePeer("@BotFather")
			if err != nil {
				results <- collectBotTokensResult{name: name, err: fmt.Errorf("解析 @BotFather 失败: %w", err)}
				return
			}
			me, err := client.GetMe()
			if err != nil {
				results <- collectBotTokensResult{name: name, err: fmt.Errorf("获取 UserBot 信息失败: %w", err)}
				return
			}
			lastSeen := infos.latestBotFatherReplyID(client, peer, me.ID)
			tokens, err := infos.collectBotTokensFromSingleUser(name, client, peer, me.ID, &lastSeen)
			if err != nil {
				results <- collectBotTokensResult{name: name, tokens: tokens, err: fmt.Errorf("通过 /token 获取 BotToken 失败: %w", err)}
				return
			}
			results <- collectBotTokensResult{name: name, tokens: tokens}
		}(name, client)
	}
	go func() {
		wg.Wait()
		close(results)
	}()

	unique := make(map[string]struct{})
	allTokens := make([]string, 0, 16)
	successUsers := 0
	failedUsers := 0
	for result := range results {
		if result.err != nil {
			failedUsers++
			debugf("UserBot[%s] 获取失败: %v", result.name, result.err)
			if len(result.tokens) == 0 {
				continue
			}
		}
		if result.err == nil {
			successUsers++
		}
		for _, token := range result.tokens {
			if _, ok := unique[token]; ok {
				continue
			}
			unique[token] = struct{}{}
			allTokens = append(allTokens, token)
			infos.appendBotToken(token)
		}
		log.Printf("UserBot[%s] 获取完成，发现 token=%d", result.name, len(result.tokens))
	}

	if len(allTokens) == 0 {
		return nil, fmt.Errorf("未从任何 UserBot 账号获取到 token")
	}
	log.Printf("所有 UserBot 获取完成：成功账号=%d 失败账号=%d 去重后 token 总数=%d", successUsers, failedUsers, len(allTokens))
	return allTokens, nil
}

func (infos *Infos) collectBotTokensFromSingleUser(name string, client *telegram.Client, peer any, actorID int64, lastSeen *int32) ([]string, error) {
	if _, err := infos.botFatherSendAndWait(client, peer, actorID, "/cancel", lastSeen, 12*time.Second); err != nil {
		debugf("BotFather /cancel 失败，继续尝试: user=%s err=%v", name, err)
	}

	resp, err := infos.botFatherSendAndExpect(client, peer, actorID, "/mybots", lastSeen, botFatherStep{
		containsAny: []string{"choose a bot from the list below", "choose a bot"},
		timeout:     20 * time.Second,
	})
	if err != nil {
		return nil, err
	}

	usernames := infos.collectBotUsernamesFromMyBots(name, client, peer, actorID, resp)
	if len(usernames) == 0 {
		return nil, fmt.Errorf("BotFather 未返回 bot 列表")
	}

	seen := make(map[string]struct{}, len(usernames))
	tokens := make([]string, 0, len(usernames))
	for _, username := range usernames {
		if _, ok := seen[username]; ok {
			continue
		}
		seen[username] = struct{}{}
		if _, err := infos.botFatherSendAndExpect(client, peer, actorID, "/token", lastSeen, botFatherStep{
			containsAny: []string{"choose a bot to generate a new token", "choose a bot", "send me the username of your bot"},
			timeout:     20 * time.Second,
		}); err != nil {
			return tokens, fmt.Errorf("进入 /token 流程失败: bot=%s err=%w", username, err)
		}

		reply, err := infos.botFatherSendAndExpect(client, peer, actorID, username, lastSeen, botFatherStep{
			containsAny: []string{"use this token to access the http api", "you can use this token to access http api"},
			timeout:     20 * time.Second,
		})
		if err != nil {
			return tokens, fmt.Errorf("获取 %s token 失败: %w", username, err)
		}
		contentRaw := extractMessageContent(reply)
		if token := botTokenPattern.FindString(contentRaw); token != "" {
			tokens = append(tokens, token)
			debugf("获取 BotToken 成功: user=%s bot=%s", name, username)
			continue
		}
		return tokens, fmt.Errorf("BotFather 未返回 %s 的有效 token: %q", username, contentRaw)
	}

	_, _ = infos.botFatherSendAndWait(client, peer, actorID, "/cancel", lastSeen, 12*time.Second)
	return tokens, nil
}

func (infos *Infos) collectBotUsernamesFromMyBots(name string, client *telegram.Client, peer any, actorID int64, firstPage telegram.NewMessage) []string {
	seen := make(map[string]struct{})
	usernames := make([]string, 0, 16)
	page := firstPage
	visitedPages := make(map[string]struct{})

	for pageIndex := 0; pageIndex < 100; pageIndex++ {
		pageSignature := botFatherMarkupSignature(page)
		if pageSignature != "" {
			if _, ok := visitedPages[pageSignature]; ok {
				break
			}
			visitedPages[pageSignature] = struct{}{}
		}

		pageUsernames := extractBotUsernamesFromMarkup(page)
		for _, username := range pageUsernames {
			if _, ok := seen[username]; ok {
				continue
			}
			seen[username] = struct{}{}
			usernames = append(usernames, username)
		}

		nextData, nextText, ok := extractBotFatherNextPageButton(page)
		if !ok {
			break
		}
		if _, err := page.Click(nextData); err != nil {
			debugf("BotFather 点击下一页失败: user=%s page=%d button=%q err=%v", name, pageIndex+1, nextText, err)
			break
		}
		nextPage, err := infos.waitBotFatherEditedMessage(client, peer, actorID, page.ID, pageSignature, 12*time.Second)
		if err != nil {
			debugf("BotFather 等待下一页失败: user=%s page=%d button=%q err=%v", name, pageIndex+1, nextText, err)
			break
		}
		page = nextPage
	}

	debugf("BotFather bot 列表收集完成: user=%s pages=%d bots=%d", name, len(visitedPages), len(usernames))
	return usernames
}

func extractBotUsernamesFromMarkup(msg telegram.NewMessage) []string {
	markup := msg.ReplyMarkup()
	if markup == nil {
		return nil
	}
	inline, ok := (*markup).(*telegram.ReplyInlineMarkup)
	if !ok || inline == nil {
		return nil
	}
	seen := make(map[string]struct{})
	usernames := make([]string, 0, 8)
	for _, row := range inline.Rows {
		if row == nil {
			continue
		}
		for _, button := range row.Buttons {
			callback, ok := button.(*telegram.KeyboardButtonCallback)
			if !ok || callback == nil {
				continue
			}
			text := strings.TrimSpace(callback.Text)
			if !strings.HasPrefix(text, "@") {
				continue
			}
			if _, ok := seen[text]; ok {
				continue
			}
			seen[text] = struct{}{}
			usernames = append(usernames, text)
		}
	}
	return usernames
}

func extractBotFatherNextPageButton(msg telegram.NewMessage) ([]byte, string, bool) {
	markup := msg.ReplyMarkup()
	if markup == nil {
		return nil, "", false
	}
	inline, ok := (*markup).(*telegram.ReplyInlineMarkup)
	if !ok || inline == nil {
		return nil, "", false
	}
	for _, row := range inline.Rows {
		if row == nil {
			continue
		}
		for _, button := range row.Buttons {
			callback, ok := button.(*telegram.KeyboardButtonCallback)
			if !ok || callback == nil {
				continue
			}
			text := strings.TrimSpace(callback.Text)
			lower := strings.ToLower(text)
			if strings.Contains(lower, "next") || strings.Contains(text, "下一页") || strings.Contains(text, "下页") || strings.Contains(text, "»") || strings.Contains(text, "›") || strings.Contains(text, "➡") || strings.Contains(text, "→") {
				data := append([]byte(nil), callback.Data...)
				return data, text, true
			}
		}
	}
	return nil, "", false
}

func botFatherMarkupSignature(msg telegram.NewMessage) string {
	markup := msg.ReplyMarkup()
	if markup == nil {
		return extractMessageContent(msg)
	}
	inline, ok := (*markup).(*telegram.ReplyInlineMarkup)
	if !ok || inline == nil {
		return extractMessageContent(msg)
	}
	var b strings.Builder
	b.WriteString(extractMessageContent(msg))
	for _, row := range inline.Rows {
		if row == nil {
			continue
		}
		b.WriteByte('|')
		for _, button := range row.Buttons {
			callback, ok := button.(*telegram.KeyboardButtonCallback)
			if !ok || callback == nil {
				continue
			}
			b.WriteString(strings.TrimSpace(callback.Text))
			b.WriteByte(':')
			b.WriteString(string(callback.Data))
			b.WriteByte(',')
		}
	}
	return b.String()
}

func (infos *Infos) waitBotFatherEditedMessage(client *telegram.Client, peer any, actorID int64, messageID int32, previousSignature string, timeout time.Duration) (telegram.NewMessage, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		msgs, err := client.GetMessages(peer, &telegram.SearchOption{Limit: 10})
		if err == nil {
			for idx := range msgs {
				msg := msgs[idx]
				if msg.SenderID() == actorID {
					continue
				}
				if msg.ID != messageID {
					continue
				}
				if sig := botFatherMarkupSignature(msg); sig != "" && sig != previousSignature {
					return msg, nil
				}
			}
		}
		time.Sleep(800 * time.Millisecond)
	}
	return telegram.NewMessage{}, fmt.Errorf("等待 BotFather 翻页消息更新超时")
}

func (infos *Infos) firstConfiguredUserClient() (UserBot, *telegram.Client, error) {
	accounts := infos.Conf.EffectiveUserBots()
	if len(accounts) == 0 {
		return UserBot{}, nil, fmt.Errorf("未配置 userBots")
	}
	account := accounts[0]
	key := configuredUserClientKey(account, 0)

	infos.Mutex.RLock()
	client := infos.UserClients[key]
	infos.Mutex.RUnlock()
	if client != nil {
		return account, client, nil
	}
	if infos.UserClient != nil {
		return account, infos.UserClient, nil
	}
	return UserBot{}, nil, fmt.Errorf("首个 UserBot 未初始化: %s", key)
}

func configuredUserClientKey(account UserBot, idx int) string {
	phone := strings.TrimSpace(account.Phone)
	if phone != "" {
		return sanitizeSessionName(phone)
	}
	name := strings.TrimSpace(account.Name)
	if name == "" {
		name = fmt.Sprintf("user%d", idx+1)
	}
	return sanitizeSessionName(name)
}

func (infos *Infos) createSingleBot(client *telegram.Client, peer any, actorID int64, lastSeen *int32) (string, string, error) {
	if _, err := infos.botFatherSendAndExpect(client, peer, actorID, "/newbot", lastSeen, botFatherStep{
		containsAny: []string{"choose a name for your bot", "how are we going to call it"},
		timeout:     20 * time.Second,
	}); err != nil {
		return "", "", err
	}

	username := randomBotUsername()
	displayName := username
	if _, err := infos.botFatherSendAndExpect(client, peer, actorID, displayName, lastSeen, botFatherStep{
		containsAny: []string{"choose a username for your bot", "must end in `bot`", "must end in 'bot'", "must end in bot"},
		timeout:     20 * time.Second,
	}); err != nil {
		return "", "", err
	}

	for attempt := 0; attempt < 5; attempt++ {
		if attempt > 0 {
			username = randomBotUsername()
		}
		reply, err := infos.botFatherSendAndWait(client, peer, actorID, username, lastSeen, 25*time.Second)
		if err != nil {
			return "", "", err
		}
		text := strings.ToLower(extractMessageContent(reply))
		if token := botTokenPattern.FindString(text); token != "" {
			return token, username, nil
		}
		if strings.Contains(text, "done! congratulations") || strings.Contains(text, "use this token to access the http api") {
			if token := botTokenPattern.FindString(extractMessageContent(reply)); token != "" {
				return token, username, nil
			}
		}
		if strings.Contains(text, "username is already taken") || strings.Contains(text, "sorry, this username is already taken") || strings.Contains(text, "username is invalid") {
			debugf("BotFather 用户名不可用，重试: username=%s reply=%q", username, text)
			continue
		}
		if strings.Contains(text, "i can help you create and manage telegram bots") {
			debugf("BotFather 返回帮助页，重新进入 /newbot 流程: username=%s", username)
			return "", "", fmt.Errorf("BotFather 会话已重置，请重试")
		}
		debugf("BotFather 未返回 token，重试用户名: username=%s reply=%q", username, text)
	}

	_, _ = infos.botFatherSendAndWait(client, peer, actorID, "/cancel", lastSeen, 12*time.Second)
	return "", "", fmt.Errorf("BotFather 未返回有效 token")
}

func (infos *Infos) botFatherSendAndWait(client *telegram.Client, peer any, actorID int64, text string, lastSeen *int32, timeout time.Duration) (telegram.NewMessage, error) {
	baseline := infos.latestBotFatherReplyID(client, peer, actorID)
	if lastSeen != nil && *lastSeen > baseline {
		baseline = *lastSeen
	}
	msg, err := client.SendMessage(peer, text, nil)
	if err != nil {
		return telegram.NewMessage{}, fmt.Errorf("发送消息 %q 失败: %w", text, err)
	}
	afterID := int32(0)
	if msg != nil {
		afterID = msg.ID
	}
	if baseline > afterID {
		afterID = baseline
	}
	resp, err := infos.waitBotFatherReply(client, peer, actorID, afterID, timeout)
	if err != nil {
		return telegram.NewMessage{}, err
	}
	if lastSeen != nil && resp.ID > *lastSeen {
		*lastSeen = resp.ID
	}
	return resp, nil
}

func (infos *Infos) waitBotFatherReply(client *telegram.Client, peer any, actorID int64, afterID int32, timeout time.Duration) (telegram.NewMessage, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		msgs, err := client.GetMessages(peer, &telegram.SearchOption{Limit: 10})
		if err == nil {
			var latest *telegram.NewMessage
			for idx := range msgs {
				msg := msgs[idx]
				if msg.ID <= afterID {
					continue
				}
				if msg.SenderID() == actorID {
					continue
				}
				if latest == nil || msg.ID > latest.ID {
					copyMsg := msg
					latest = &copyMsg
				}
			}
			if latest != nil {
				return *latest, nil
			}
		}
		time.Sleep(800 * time.Millisecond)
	}
	return telegram.NewMessage{}, fmt.Errorf("等待 BotFather 回复超时")
}

func (infos *Infos) appendBotToken(token string) {
	infos.Mutex.Lock()
	defer infos.Mutex.Unlock()
	for _, existing := range infos.Conf.BotTokens {
		if existing == token {
			return
		}
	}
	infos.Conf.BotTokens = append(infos.Conf.BotTokens, token)
}

func randomAlphaNumeric(n int) string {
	if n <= 0 {
		return ""
	}
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	buf := make([]byte, n)
	randBuf := make([]byte, n)
	if _, err := rand.Read(randBuf); err != nil {
		for i := range buf {
			buf[i] = chars[time.Now().UnixNano()%int64(len(chars))]
			time.Sleep(time.Nanosecond)
		}
		return string(buf)
	}
	for i := range buf {
		buf[i] = chars[int(randBuf[i])%len(chars)]
	}
	return string(buf)
}

func randomBotUsername() string {
	const firstChars = "abcdefghijklmnopqrstuvwxyz"
	first := randomFromCharset(firstChars, 1)
	body := randomAlphaNumeric(9)
	return first + body + "_bot"
}

func randomFromCharset(charset string, n int) string {
	if n <= 0 || charset == "" {
		return ""
	}
	buf := make([]byte, n)
	randBuf := make([]byte, n)
	if _, err := rand.Read(randBuf); err != nil {
		for i := range buf {
			buf[i] = charset[time.Now().UnixNano()%int64(len(charset))]
			time.Sleep(time.Nanosecond)
		}
		return string(buf)
	}
	for i := range buf {
		buf[i] = charset[int(randBuf[i])%len(charset)]
	}
	return string(buf)
}

func (infos *Infos) latestBotFatherReplyID(client *telegram.Client, peer any, actorID int64) int32 {
	msgs, err := client.GetMessages(peer, &telegram.SearchOption{Limit: 10})
	if err != nil {
		return 0
	}
	latest := int32(0)
	for idx := range msgs {
		msg := msgs[idx]
		if msg.SenderID() == actorID {
			continue
		}
		if msg.ID > latest {
			latest = msg.ID
		}
	}
	return latest
}

func (infos *Infos) botFatherSendAndExpect(client *telegram.Client, peer any, actorID int64, text string, lastSeen *int32, step botFatherStep) (telegram.NewMessage, error) {
	for {
		resp, err := infos.botFatherSendAndWait(client, peer, actorID, text, lastSeen, step.timeout)
		if err != nil {
			return telegram.NewMessage{}, err
		}
		contentRaw := extractMessageContent(resp)
		content := strings.ToLower(contentRaw)
		if waitSeconds, ok := parseBotFatherTooManyAttempts(contentRaw); ok {
			infos.sleepWithRefresh(waitSeconds)
			continue
		}
		for _, expected := range step.containsAny {
			if strings.Contains(content, strings.ToLower(expected)) {
				return resp, nil
			}
		}
		return telegram.NewMessage{}, fmt.Errorf("BotFather 回复不符合预期: %q", contentRaw)
	}
}

func parseBotFatherTooManyAttempts(text string) (int, bool) {
	matches := botFatherTooManyAttemptsPattern.FindStringSubmatch(strings.TrimSpace(text))
	if len(matches) != 2 {
		return 0, false
	}
	seconds, err := strconv.Atoi(matches[1])
	if err != nil || seconds <= 0 {
		return 0, false
	}
	return seconds + 1, true
}

func (infos *Infos) sleepWithRefresh(waitSeconds int) {
	if waitSeconds <= 0 {
		return
	}
	debugf("BotFather 限流，等待开始: wait=%ds", waitSeconds)
	writer := log.Writer()
	remaining := waitSeconds
	for remaining > 0 {
		_, _ = fmt.Fprintf(writer, "\rBotFather 限流等待中: remain=%ds (%s)    ", remaining, handleTime(uint64(remaining)))
		time.Sleep(1 * time.Second)
		remaining--
	}
	_, _ = fmt.Fprintln(writer)
	debugf("BotFather 限流等待结束，继续重试")
}
