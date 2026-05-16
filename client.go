package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/amarnathcjd/gogram/telegram"
)

func (infos *Infos) clientNameForTask(task DownloadChannel) (string, *telegram.Client) {
	user := strings.TrimSpace(task.User)
	if user != "" {
		if client, ok := infos.UserClients[user]; ok {
			return user, client
		}
		return "", nil
	}
	if infos.DefaultUserName != "" {
		if client, ok := infos.UserClients[infos.DefaultUserName]; ok {
			return infos.DefaultUserName, client
		}
	}
	if infos.UserClient != nil {
		// we don't have the name for single UserClient, try find by pointer
		for name, c := range infos.UserClients {
			if c == infos.UserClient {
				return name, c
			}
		}
		return "default", infos.UserClient
	}
	for name, client := range infos.UserClients {
		if client != nil {
			return name, client
		}
	}
	return "", nil
}

// tryJoinChannel 使用 JoinChannel 按用户名、邀请链接或可解析 peer 加入频道
func tryJoinChannel(client *telegram.Client, joinTarget string) error {
	joinTarget = strings.TrimSpace(joinTarget)
	if joinTarget == "" {
		return fmt.Errorf("缺少 join 目标，无法强制加入频道")
	}
	_, err := client.JoinChannel(joinTarget)
	return err
}

func sanitizeSessionName(name string) string {
	name = strings.TrimSpace(strings.ToLower(name))
	if name == "" {
		return "default"
	}
	b := strings.Builder{}
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_', r == '-':
			b.WriteRune(r)
		case r == ' ':
			b.WriteRune('_')
		}
	}
	if b.Len() == 0 {
		return "default"
	}
	return b.String()
}

func (infos *Infos) userClientConf(name string, dc int) telegram.ClientConfig {
	tag := sanitizeSessionName(name)
	conf := botConf("user_" + tag)
	if dc != 0 {
		conf.DataCenter = dc
	}
	return conf
}

func (infos *Infos) loginViaTerminal(client *telegram.Client, account UserBot) error {
	reader := bufio.NewReader(os.Stdin)
	phone := strings.TrimSpace(account.Phone)
	if phone == "" {
		fmt.Printf("请输入 UserBot[%s] 的手机号(带国家码, 如 +8613800000000): ", account.Name)
		value, readErr := reader.ReadString('\n')
		if readErr != nil {
			return readErr
		}
		phone = strings.TrimSpace(value)
		if phone == "" {
			return errors.New("手机号为空，无法登录")
		}
	}
	if !strings.HasPrefix(phone, "+") {
		phone = "+" + phone
	}
	log.Printf("UserBot[%s] 未登录，开始终端登录流程: %s", account.Name, phone)
	_, err := client.Login(phone, &telegram.LoginOptions{
		CodeCallback: func() (string, error) {
			fmt.Printf("请输入 UserBot[%s] 的验证码: ", account.Name)
			code, readErr := reader.ReadString('\n')
			if readErr != nil {
				return "", readErr
			}
			var sb strings.Builder
			for _, r := range code {
				if r >= '0' && r <= '9' {
					sb.WriteRune(r)
				}
			}
			value := sb.String()
			if value == "" {
				return "", errors.New("验证码为空")
			}
			return value, nil
		},
		PasswordCallback: func() (string, error) {
			if account.Password != "" {
				return account.Password, nil
			}
			fmt.Printf("请输入 UserBot[%s] 的 2FA 密码: ", account.Name)
			pass, readErr := reader.ReadString('\n')
			if readErr != nil {
				return "", readErr
			}
			pass = strings.TrimSpace(pass)
			if pass == "" {
				return "", errors.New("2FA 密码为空")
			}
			return pass, nil
		},
		MaxRetries: 3,
	})
	if err != nil {
		return err
	}
	return nil
}

func (infos *Infos) initUserClientsForDownloadOnly() error {
	accounts := infos.Conf.EffectiveDownloadUserBots()
	if len(accounts) == 0 {
		// try to auto-load sessions from sessions/ directory
		loaded := infos.loadSessionsDirClients()
		if len(loaded) > 0 {
			return nil
		}
		return errors.New("未配置 userBots，且默认账号为空")
	}

	infos.Mutex.Lock()
	if infos.UserClients == nil {
		infos.UserClients = make(map[string]*telegram.Client, len(accounts))
	}
	infos.Mutex.Unlock()

	var wg sync.WaitGroup
	var failMutex sync.Mutex
	failCount := 0

	for idx, account := range accounts {
		wg.Add(1)
		go func(idx int, origAccount UserBot) {
			defer wg.Done()
			account := origAccount
		// 优先使用手机号作为会话名（如果存在），否则使用配置的 name，若都不存在则使用 userN
		phone := strings.TrimSpace(account.Phone)
		var name string
		if phone != "" {
			name = sanitizeSessionName(phone)
		} else {
			name = strings.TrimSpace(account.Name)
			if name == "" {
				name = fmt.Sprintf("user%d", idx+1)
			}
			name = sanitizeSessionName(name)
		}
		account.Name = name

			client, err := telegram.NewClient(infos.userClientConf(account.Name, account.DC))
			if err != nil {
				log.Printf("创建 UserBot[%s] 失败: %v", account.Name, err)
				failMutex.Lock()
				failCount++
				failMutex.Unlock()
				return
			}
			if err = client.Connect(); err != nil {
				log.Printf("连接 UserBot[%s] 失败: %v", account.Name, err)
				failMutex.Lock()
				failCount++
				failMutex.Unlock()
				return
			}

			me, meErr := client.GetMe()
			if meErr != nil {
				if strings.Contains(strings.ToUpper(meErr.Error()), "AUTH_KEY_UNREGISTERED") {
					if err = infos.loginViaTerminal(client, account); err != nil {
						log.Printf("UserBot[%s] 登录失败: %v", account.Name, err)
						failMutex.Lock()
						failCount++
						failMutex.Unlock()
						return
					}
					me, meErr = client.GetMe()
				}
				if meErr != nil {
					log.Printf("获取 UserBot[%s] 信息失败: %v", account.Name, meErr)
					failMutex.Lock()
					failCount++
					failMutex.Unlock()
					return
				}
			}

			if account.UserID != 0 && me.ID != account.UserID {
				log.Printf("UserBot[%s] 账号不匹配: 配置 %d, 实际 %d", account.Name, account.UserID, me.ID)
				failMutex.Lock()
				failCount++
				failMutex.Unlock()
				return
			}

			infos.Mutex.Lock()
			infos.UserClients[account.Name] = client
			infos.UserClientIDs[account.Name] = me.ID
			if infos.DefaultUserName == "" {
				infos.DefaultUserName = account.Name
				infos.UserClient = client
			}
			infos.Mutex.Unlock()

		}(idx, account)
	}
	wg.Wait()

	if infos.UserClient == nil {
		if failCount > 0 {
			return errors.New("所有 UserBot 初始化失败")
		}
		return errors.New("未找到可用 UserBot 客户端")
	}
	infos.Status.Store(3)
	return nil
}

// loadSessionsDirClients 扫描 sessions/ 目录并加载所有以 .session 结尾的会话文件为 UserClients
func (infos *Infos) loadSessionsDirClients() map[string]*telegram.Client {
	sessionsDir := filepath.Join(infos.FilesPath, "sessions")
	if err := os.MkdirAll(sessionsDir, 0755); err != nil {
		log.Printf("创建 sessions 目录失败: %v", err)
		return nil
	}
	files, err := os.ReadDir(sessionsDir)
	if err != nil {
		log.Printf("读取 sessions 目录失败: %v", err)
		return nil
	}
	loaded := make(map[string]*telegram.Client)
	for _, fi := range files {
		if fi.IsDir() {
			continue
		}
		name := fi.Name()
		if !strings.HasSuffix(name, ".session") {
			continue
		}
		base := strings.TrimSuffix(name, ".session")
		// only consider user sessions (prefix user) to avoid loading bot session here
		if !strings.HasPrefix(strings.ToLower(base), "user") {
			continue
		}
		tag := strings.TrimPrefix(base, "user_")
		if tag == "" {
			tag = "default"
		}
		// create client using same botConf naming
		client, err := telegram.NewClient(infos.userClientConf(base, 0))
		if err != nil {
			log.Printf("为会话 %s 创建客户端失败: %v", name, err)
			continue
		}
		if err := client.Connect(); err != nil {
			log.Printf("连接会话 %s 的客户端失败: %v", name, err)
			continue
		}
		me, meErr := client.GetMe()
		if meErr != nil {
			log.Printf("获取会话 %s 用户信息失败: %v", name, meErr)
			continue
		}
		infos.Mutex.Lock()
		if infos.UserClients == nil {
			infos.UserClients = make(map[string]*telegram.Client)
		}
		infos.UserClients[tag] = client
		if infos.UserClientIDs == nil {
			infos.UserClientIDs = make(map[string]int64)
		}
		infos.UserClientIDs[tag] = me.ID
		if infos.DefaultUserName == "" {
			infos.DefaultUserName = tag
			infos.UserClient = client
		}
		infos.Mutex.Unlock()
		log.Printf("从 sessions 加载 UserBot: %s uid=%d", base, me.ID)
		loaded[tag] = client
	}
	return loaded
}

// startBot 创建并连接所有 Bot 客户端, 注册消息处理器并设置命令菜单
func (infos *Infos) startBot() (err error) {
	clients := make([]*telegram.Client, 0, len(infos.Conf.BotTokens))
	relayBotIDs := make([]int64, 0, len(infos.Conf.BotTokens))
	relayBotLabels := make([]string, 0, len(infos.Conf.BotTokens))
	relayBotTargets := make([]string, 0, len(infos.Conf.BotTokens))
	siteConfigured := strings.TrimSpace(infos.Conf.Site) != ""
	printedSiteSkip := false
	type botStartupResult struct {
		client *telegram.Client
		me     *telegram.UserObj
		err    error
	}
	results := make([]botStartupResult, len(infos.Conf.BotTokens))
	var wg sync.WaitGroup
	for idx, token := range infos.Conf.BotTokens {
		wg.Add(1)
		go func(idx int, token string) {
			defer wg.Done()

			token = strings.TrimSpace(token)
			sessionName := fmt.Sprintf("bot_%d", idx+1)
			if parts := strings.SplitN(token, ":", 2); len(parts) > 0 {
				if botID := strings.TrimSpace(parts[0]); botID != "" {
					sessionName = fmt.Sprintf("bot_%s", botID)
				}
			}

			client, createErr := telegram.NewClient(botConf(sessionName))
			if createErr != nil {
				results[idx].err = fmt.Errorf("创建 Bot[%d] 客户端失败: %w", idx+1, createErr)
				return
			}

			if connectErr := client.Connect(); connectErr != nil {
				results[idx].err = fmt.Errorf("Bot[%d] 连接失败: %w", idx+1, connectErr)
				return
			}

			if loginErr := client.LoginBot(token); loginErr != nil {
				results[idx].err = fmt.Errorf("Bot[%d] 登录失败: %w", idx+1, loginErr)
				return
			}

			me, meErr := client.GetMe()
			if meErr != nil {
				results[idx].err = fmt.Errorf("获取 Bot[%d] 信息失败: %w", idx+1, meErr)
				return
			}

			results[idx].client = client
			results[idx].me = me
		}(idx, token)
	}
	wg.Wait()

	for idx, result := range results {
		if result.err != nil {
			for _, started := range results {
				if started.client != nil {
					if disconnectErr := started.client.Disconnect(); disconnectErr != nil {
						log.Printf("Bot[%d] 回滚断开失败: %+v", idx+1, disconnectErr)
					}
				}
			}
			log.Printf("%+v", result.err)
			return result.err
		}

		client := result.client
		me := result.me

		// 始终注册入站媒体捕获器，供下载分流链路使用
		client.On(telegram.OnMessage, handleRelayInboxCapture)

		// 仅第一个 Bot 负责消息监听与命令注册
		if idx == 0 && siteConfigured {
			client.On(telegram.OnMessage, handleBotCommand)
			infos.setupBotCommands(client)
		} else if idx == 0 && !siteConfigured && !printedSiteSkip {
			log.Printf("未配置 site，跳过消息监听与命令注册")
			printedSiteSkip = true
		}
		clients = append(clients, client)
		relayBotIDs = append(relayBotIDs, me.ID)
		relayBotLabels = append(relayBotLabels, fmt.Sprintf("bot%d", idx+1))
		if me.Username != "" {
			relayBotTargets = append(relayBotTargets, "@"+me.Username)
		} else {
			relayBotTargets = append(relayBotTargets, "")
		}
	}

	infos.Mutex.Lock()
	infos.BotClients = clients
	infos.RelayBotClients = clients
	infos.RelayBotIDs = relayBotIDs
	infos.RelayBotLabels = relayBotLabels
	infos.RelayBotTargets = relayBotTargets
	if len(clients) > 0 {
		infos.BotClient = clients[0]
	}
	infos.Mutex.Unlock()
	return nil
}

func (infos *Infos) setupBotCommands(client *telegram.Client) {
	go func() {
		// 先清空默认的命令列表, 确保没有权限的用户什么也看不到
		_, err := client.SetBotCommands([]*telegram.BotCommand{}, nil)
		if err != nil {
			log.Printf("清空默认命令失败: %+v", err)
		}

		if infos.Conf.UserID == 0 {
			log.Printf("userID=0，跳过为管理员设置 Bot 命令；请先登录 UserBot 或手动填写 userID")
			return
		}

		userID, err := client.ResolvePeer(infos.Conf.UserID)
		if err != nil {
			log.Printf("解析用户 ID 失败: %v", err)
			return
		}
		commands := []*telegram.BotCommand{
			{
				Command:     "qr",
				Description: "获取登录二维码",
			},
			{
				Command:     "phone",
				Description: "输入手机号登录",
			},
			{
				Command:     "code",
				Description: "输入验证码登录(需混入非数字字符)",
			},
			{
				Command:     "pass",
				Description: "输入2FA密码登录",
			},
		}
		commonCommands := []*telegram.BotCommand{
			{
				Command:     "dc",
				Description: "设置客户端默认DC",
			},
			{
				Command:     "allow",
				Description: "添加白名单",
			},
			{
				Command:     "disallow",
				Description: "移除白名单",
			},
			{
				Command:     "add",
				Description: "添加搜索频道",
			},
			{
				Command:     "del",
				Description: "移除搜索频道",
			},
			{
				Command:     "addrule",
				Description: "添加关键词规则",
			},
			{
				Command:     "delrule",
				Description: "移除关键词规则",
			},
			{
				Command:     "list",
				Description: "列出搜索频道、白名单、关键词规则",
			},
			{
				Command:     "info",
				Description: "获取程序运行信息",
			},
			{
				Command:     "size",
				Description: "设置程序缓存大小",
			},
			{
				Command:     "site",
				Description: "设置反代域名",
			},
			{
				Command:     "port",
				Description: "设置HTTP服务端口",
			},
			{
				Command:     "proxy",
				Description: "设置代理",
			},
			{
				Command:     "check",
				Description: "查找HASH对应的用户信息",
			},
			{
				Command:     "workers",
				Description: "设置并发数",
			},
			{
				Command:     "channel",
				Description: "设置绑定频道",
			},
			{
				Command:     "password",
				Description: "设置接口访问密码",
			},
		}
		commands = append(commands, commonCommands...)

		_, err = client.SetBotCommands(commands, &userID)
		if err != nil {
			log.Printf("设置 Bot 超级管理员命令失败: %+v", err)
			return
		}

		for _, adminID := range infos.Conf.AdminIDs {
			if adminID == infos.Conf.UserID {
				continue
			}
			userID, err := client.ResolvePeer(adminID)
			if err != nil {
				log.Printf("解析用户 ID 失败: %v", err)
				continue
			}
			_, err = client.SetBotCommands(commonCommands, &userID)
			if err != nil {
				log.Printf("设置 Bot 管理员命令失败: %+v", err)
				continue
			}
		}
	}()
}

// userBotClient 创建并连接 UserBot 客户端（不执行登录, 仅建立连接）
func (infos *Infos) userBotClient() (err error) {
	conf := botConf("user")
	if infos.Conf.DC != 0 {
		conf.DataCenter = infos.Conf.DC
	}

	client, err := telegram.NewClient(conf)
	if err != nil {
		log.Printf("创建 UserBot 客户端失败: %+v", err)
		return
	}

	// 连接 UserBot
	if err = client.Connect(); err != nil {
		log.Printf("UserBot 连接失败: %+v", err)
		return
	}

	infos.Mutex.Lock()
	infos.UserClient = client
	infos.Mutex.Unlock()

	return err
}

// startUserBot 发起手机号登录流程
func (infos *Infos) startUserBot(phone string) (err error) {
	infos.Mutex.Lock()
	switch infos.Status.Load() {
	case 1, 2:
		// 正在进行验证码或密码输入状态, 不允许重复发起
		infos.Mutex.Unlock()
		err = errors.New("已有登录流程正在进行")
		log.Printf("UserBot 登录失败: %+v", err)
		return err
	case 3:
		// 已登录状态, 若客户端实例丢失则尝试重建
		infos.Mutex.Unlock()
		if infos.UserClient == nil {
			if err := infos.userBotClient(); err != nil {
				log.Printf("UserBot 登录失败: %+v", err)
				infos.resetStatus()
				return err
			}
		}
		return nil
	default:
		// 未登录状态, 开始新的登录流程
		infos.Mutex.Unlock()
		if infos.UserClient == nil {
			if err := infos.userBotClient(); err != nil {
				log.Printf("UserBot 登录失败: %+v", err)
				infos.resetStatus()
				return err
			}
		}
		sendMS(nil, fmt.Sprintf("收到手机号 %s, 正在尝试发送验证码...", phone), nil, 60)

		// 在协程中执行阻塞的登录命令
		go func() {
			status, err := infos.UserClient.Login(phone, &telegram.LoginOptions{
				CodeCallback:     infos.code, // 指定验证码回调函数
				PasswordCallback: infos.pass, // 指定二步验证回调函数
				MaxRetries:       3,
			})
			if err != nil {
				log.Printf("UserBot 登录失败: %+v", err)
				sendMS(nil, fmt.Sprintf("UserBot 登录失败: %+v", err), nil, 60)
				infos.resetStatus()
				return
			}

			if status == true {
				log.Printf("UserBot 登录成功")
				if err := infos.checkStatus(); err != nil {
					log.Printf("UserBot 登录失败: %+v", err)
					infos.resetStatus()
					return
				}
			}
		}()
	}

	return nil
}

// startUserBotQR 发起二维码登录流程
func (infos *Infos) startUserBotQR() (err error) {
	infos.Mutex.Lock()
	switch infos.Status.Load() {
	case 1, 2:
		infos.Mutex.Unlock()
		err = errors.New("已有登录流程正在进行")
		log.Printf("UserBot 登录失败: %+v", err)
		return err
	case 3:
		infos.Mutex.Unlock()
		if infos.UserClient == nil {
			if err := infos.userBotClient(); err != nil {
				log.Printf("UserBot 登录失败: %+v", err)
				infos.resetStatus()
				return err
			}
		}
		return nil
	default:
		infos.Status.Store(1)
		infos.Mutex.Unlock()
		if infos.UserClient == nil {
			if err := infos.userBotClient(); err != nil {
				log.Printf("UserBot 登录失败: %+v", err)
				infos.resetStatus()
				return err
			}
		}
		sendMS(nil, "正在请求登录二维码...", nil, 60)

		// 启动登录流程（会阻塞, 直到登录完成或失败）
		go func() {
			qr, err := infos.UserClient.QRLogin(telegram.QrOptions{
				PasswordCallback: infos.pass,
			})
			if err != nil {
				log.Printf("获取 QR 登录失败: %+v", err)
				if !telegram.MatchError(err, "SESSION_PASSWORD_NEEDED]") {
					sendMS(nil, fmt.Sprintf("获取 QR 登录失败: %+v", err), nil, 60)
					infos.resetStatus()
					return
				}
			}

			png, err := qr.ExportAsPng()
			if err != nil {
				log.Printf("导出 QR PNG 失败: %+v", err)
				return
			}

			src, err := infos.BotClient.UploadFile(png, &telegram.UploadOptions{
				FileName: "qr.png",
			})
			if err != nil {
				log.Printf("上传 QR 文件失败: %+v", err)
				return
			}
			sendMS(nil, src, &telegram.SendOptions{Caption: "请使用手机 Telegram 扫描此二维码登录。二维码有效期 30 秒, 如失效请重新发送 /qr"}, 35)
			err = qr.WaitLogin()
			if err != nil {
				if !strings.Contains(err.Error(), "scanning again") {
					sendMS(nil, fmt.Sprintf("QR 登录失败: %+v", err), nil, 60)
					infos.resetStatus()
					return
				}
			}

			if err := infos.checkStatus(); err != nil {
				log.Printf("UserBot 登录失败: %+v", err)
				infos.resetStatus()
				return
			}
		}()
	}

	return nil
}

// checkStatus 获取当前 UserBot 登录状态并校验 ID 是否合法
func (infos *Infos) checkStatus() (err error) {
	// 登录成功
	me, err := infos.UserClient.GetMe()
	if err != nil {
		log.Printf("获取用户信息失败: %v", err)
		infos.Mutex.Lock()
		infos.Status.Store(0)
		infos.Mutex.Unlock()
		return nil
	}

	if infos.Conf.UserID == 0 {
		infos.Mutex.Lock()
		infos.Conf.UserID = me.ID
		infos.IDs[me.ID] = ID{IsAdmin: true, IsWhite: true}
		if saveErr := saveConf(infos.Conf, infos.FilesPath); saveErr != nil {
			log.Printf("自动写入 userID 失败: %+v", saveErr)
		}
		infos.Mutex.Unlock()
		log.Printf("检测到 userID 未配置，已自动设置为当前 UserBot: %d", me.ID)
	}

	if me.ID == infos.Conf.UserID {
		name := me.FirstName + me.LastName
		if me.Username != "" {
			name = "@" + me.Username
		}
		sendMS(nil, fmt.Sprintf("登录成功! 用户: %s", name), nil)
		infos.Mutex.Lock()
		infos.Status.Store(3)
		infos.Mutex.Unlock()
		if infos.BotClient != nil {
			go infos.startConfiguredDownloads(context.Background())
		} else {
			infos.startConfiguredDownloads(context.Background())
		}
		return nil
	} else {
		log.Printf("登录失败: 用户ID不匹配, 期望 %d, 实际 %d", infos.Conf.UserID, me.ID)
		if infos.UserClient != nil {
			if err := infos.UserClient.Disconnect(); err != nil {
				log.Printf("UserBot 退出失败: %+v", err)
			}
		}
		infos.resetStatus()
		return infos.userBotClient()
	}
}

// resetStatus 断开 UserBot 连接并清理 cache, 将状态重置为未登录
func (infos *Infos) resetStatus() {
	// 1. 断开连接并清理句柄
	if infos.UserClient != nil {
		if err := infos.UserClient.Disconnect(); err != nil {
			log.Printf("UserBot 断开连接失败: %+v", err)
		}
	}
	// 2. 重置内存状态
	infos.Mutex.Lock()
	infos.UserClient = nil
	infos.Status.Store(0)
	infos.Mutex.Unlock()
}

// code 是登录回调, 暂停协程等待用户通过 Bot 发送验证码
func (infos *Infos) code() (code string, err error) {
	if infos.Status.Load() == 0 {
		infos.Mutex.Lock()
		infos.Status.Store(1)
		infos.Mutex.Unlock()
		sendMS(nil, "等待用户输入 /code 验证码...", nil, 120)
		select {
		case code := <-infos.Code:
			return code, nil
		case <-time.After(2 * time.Minute):
			err = errors.New("等待验证码超时")
			sendMS(nil, err.Error(), nil, 60)
			return "", err
		}
	} else {
		err = errors.New("当前状态不是等待验证码")
		sendMS(nil, err.Error(), nil, 60)
		return "", err
	}
}

// submitCode 接收用户通过 Bot 发送的验证码并写入通道
func (infos *Infos) submitCode(str string) (err error) {
	infos.Mutex.Lock()
	defer infos.Mutex.Unlock()

	if infos.Status.Load() != 1 {
		err = errors.New("当前状态不是等待验证码")
		sendMS(nil, err.Error(), nil, 60)
		return err
	}

	// 过滤非数字字符
	var sb strings.Builder
	for _, r := range str {
		if isDigit(r) {
			sb.WriteRune(r)
		}
	}

	code := sb.String()
	infos.Code <- code
	return nil
}

// pass 是登录回调, 暂停协程等待用户通过 Bot 发送 2FA 密码
func (infos *Infos) pass() (pass string, err error) {
	if infos.Status.Load() == 1 {
		infos.Mutex.Lock()
		infos.Status.Store(2)
		infos.Mutex.Unlock()
		sendMS(nil, "等待用户输入 /pass 2FA密码...", nil, 120)
		select {
		case pass := <-infos.Pass:
			return pass, nil
		case <-time.After(2 * time.Minute):
			err = errors.New("等待2FA密码超时")
			sendMS(nil, err.Error(), nil, 60)
			return "", err
		}
	} else {
		err = errors.New("当前状态不是等待2FA密码")
		sendMS(nil, err.Error(), nil, 60)
		return "", err
	}
}

// submitPass 接收用户通过 Bot 发送的 2FA 密码并写入通道
func (infos *Infos) submitPass(pass string) (err error) {
	infos.Mutex.Lock()
	defer infos.Mutex.Unlock()

	if infos.Status.Load() != 2 {
		err = errors.New("当前状态不是等待2FA密码")
		sendMS(nil, err.Error(), nil, 60)
		return err
	}
	infos.Pass <- pass
	return nil
}

// botConf 构造 Telegram 客户端所需的通用配置
func botConf(cate string) (conf telegram.ClientConfig) {
	conf = telegram.ClientConfig{
		AppID:        infos.Conf.AppID,
		AppHash:      infos.Conf.AppHash,
		LogLevel:     telegram.LogError,
		Session:      filepath.Join(infos.FilesPath, "sessions", fmt.Sprintf("%s.session", cate)),
		Cache:        telegram.NewCache(filepath.Join(infos.FilesPath, "sessions", fmt.Sprintf("%s.cache", cate))),
		CacheSenders: true,
		DeviceConfig: telegram.DeviceConfig{
			DeviceModel:   "Android",
			SystemVersion: "Android 14",
			AppVersion:    "10.14.3",
		},
		FloodHandler: func(err error) bool {
			wait := 3
			matches := infos.Rex.FindStringSubmatch(err.Error())
			if len(matches) > 1 {
				for _, match := range matches {
					if value, err := strconv.Atoi(match); err == nil {
						wait = value
						break
					}
				}
			}
			debugf("访问太过频繁, 等待 %d 秒后重试", wait+1)
			waitUntil := time.Now().Add(time.Duration(wait+1) * time.Second)
			infos.WaitUntil.Store(waitUntil.Unix())
			time.Sleep(time.Duration(wait+1) * time.Second)
			return true
		},
	}
	if infos.Conf.Proxy != "" {
		proxy, err := telegram.ProxyFromURL(infos.Conf.Proxy)
		if err == nil {
			conf.Proxy = proxy
		} else {
			log.Printf("代理地址解析失败: %v", err)
		}
	}
	return conf
}
