package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/amarnathcjd/gogram/telegram"
)

// HackLink 结构体用于在处理提取链接时传递中间数据
type HackLink struct {
	M       *telegram.NewMessage // 原始消息对象
	UID     int64                // 发起请求的用户 ID
	Pass    string               // 可选密码
	Hash    string               // 验证哈希
	Matches [][]string           // 正则匹配到的链接信息
}

// CleanRealm 结构体用于定义清理缓存和会话的范围
type CleanRealm struct {
	Filter bool   // 是否启用过滤, 只删除特定 ID 以外的文件
	ID     string // 过滤 ID（如账号 ID）
	Cate   string // 类型：bot 或 user
	Realm  string // 范围：cache 或 session
}

type OffSet struct {
	Offset int32     // 偏移量
	Time   time.Time // 时间
}

type OffSets struct {
	Mutex   *sync.Mutex       // 互斥锁, 保护并发安全
	OffSets map[string]OffSet // 偏移量映射
}

type Item struct {
	Name string `json:"name"`
	MID  int32  `json:"mid"`
	CID  int64  `json:"cid"`
	Size int64  `json:"size"`
}

type MediaContent struct {
	Start   int64
	End     int64
	Content []byte
	Time    time.Time
}

type MediaCache struct {
	Contents []MediaContent
	Time     time.Time
}

type Items struct {
	HasMore bool   `json:"more"`
	Channel string `json:"channel"`
	Item    []Item `json:"item"`
}

type ID struct {
	Hash    string
	IsAdmin bool
	IsWhite bool
}

type RelayInboxRecord struct {
	Msg        telegram.NewMessage
	ReceivedAt int64
}

// Infos 结构体保存了程序运行时的全局状态和资源句柄
type Infos struct {
	BotClient       *telegram.Client            // 主 Bot 客户端（用于与用户交互）
	BotClients      []*telegram.Client          // 多 Bot 客户端实例
	RelayBotClients []*telegram.Client          // 可用于分流下载的 Bot 列表
	RelayBotLabels  []string                    // 与 RelayBotClients 对应的显示名称
	RelayBotIDs     []int64                     // 与 RelayBotClients 对应的 Bot 用户 ID
	RelayBotTargets []string                    // 与 RelayBotClients 对应的可解析目标（优先 @username）
	UserClient      *telegram.Client            // 全局 UserBot 客户端实例（用于读取私有内容和流式传输）
	UserClients     map[string]*telegram.Client // 多 UserBot 客户端实例
	UserClientIDs   map[string]int64            // UserBot 名称到用户 ID 的映射
	DefaultUserName string                      // 默认 UserBot 名称
	Client          *telegram.Client            // 当前活跃客户端指针
	Mutex           *sync.RWMutex               // 全局互斥锁, 保护并发安全
	Cond            *sync.Cond                  // 条件变量, 用于等待
	Conf            *Conf                       // 指向全局配置
	File            *os.File                    // 日志文件句柄
	Rex             *regexp.Regexp              // 用于解析 Telegram FloodWait 错误的正则
	FilesPath       string                      // 配置文件存放目录
	FilePath        string                      // 日志文件路径
	BotID           int64                       // 主 Bot 自身的 ID
	BotIDs          map[int64]struct{}          // 所有 Bot 自身 ID
	Status          atomic.Int32                // UserBot 登录状态: 0 未登录, 1 等待验证码, 2 等待二步验证, 3 已登录
	WaitUntil       atomic.Int64                // 等待结束时间
	Code            chan string                 // 用于接收异步提交的验证码
	Pass            chan string                 // 用于接收异步提交的二步验证密码
	IDs             map[int64]ID                // 缓存用户 ID 到哈希的映射, 减少重复计算
	HeadCache       map[string]*MediaCache      // 缓存文件头部数据
	TailCache       map[string]*MediaCache      // 缓存文件尾部数据
	DownloadStarted atomic.Bool                 // 自动下载任务是否已启动
	LastDownloaded  map[int64]int32             // 每个频道已下载到的最新消息ID
	RelayInbox      map[string]RelayInboxRecord // Bot 入站媒体缓存: key=botID:senderID
}

type colorizedWriter struct {
	w      io.Writer
	prefix string
	suffix string
}

func (cw colorizedWriter) Write(p []byte) (int, error) {
	if cw.w == nil {
		return len(p), nil
	}
	if cw.prefix != "" {
		if _, err := cw.w.Write([]byte(cw.prefix)); err != nil {
			return 0, err
		}
	}
	if _, err := cw.w.Write(p); err != nil {
		return 0, err
	}
	if cw.suffix != "" {
		if _, err := cw.w.Write([]byte(cw.suffix)); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}

var infos *Infos
var offSets *OffSets
var startTime time.Time
var version = "v1.0.10"

// main 是程序的入口函数
func main() {
	log.SetFlags(0)
	startTime = time.Now()
	// 解析命令行参数
	files := flag.String("files", "files", "配置文件所属目录路径（包含 config.yaml, session 等）")
	file := flag.String("log", "", "日志文件的存放路径")
	var ver bool
	flag.BoolVar(&ver, "version", false, "显示程序版本号并退出")
	flag.BoolVar(&ver, "v", false, "显示程序版本号并退出")
	flag.Parse()
	args := flag.Args()

	// 版本检查逻辑
	if ver {
		fmt.Println(version)
		return
	}

	// 1. 初始化全局 Infos 对象并加载配置
	value, err := newInfos(*file, *files)
	if err != nil {
		log.Printf("初始化失败: %+v", err)
		return
	}
	infos = value
	offSets = newOffSets()
	cleanAllCacheFiles()
	if err := cleanTmpDir(); err != nil {
		log.Printf("清理临时目录失败: %+v", err)
		return
	}

	if len(args) > 0 && strings.EqualFold(args[0], "makebots") {
		if err := infos.initUserClientsForDownloadOnly(); err != nil {
			log.Printf("初始化 UserBot 失败: %+v", err)
			return
		}
		if len(args) > 1 && strings.EqualFold(strings.TrimSpace(args[1]), "get") {
			tokens, err := infos.collectAllBotTokensFromAllUsers()
			if err != nil {
				log.Printf("获取机器人 token 失败: %+v", err)
				return
			}
			for _, token := range tokens {
				log.Printf("- %q", token)
			}
			return
		}

		count := 5
		if len(args) > 1 {
			parsed, parseErr := strconv.Atoi(strings.TrimSpace(args[1]))
			if parseErr != nil || parsed <= 0 {
				log.Printf("makebots 参数错误: %q，必须为正整数，或使用 get", args[1])
				return
			}
			count = parsed
		}
		tokens, err := infos.createBotsWithFirstUserBot(count)
		if err != nil {
			log.Printf("创建机器人失败: %+v", err)
			return
		}
		log.Printf("创建完成，共 %d 个", len(tokens))
		for idx, token := range tokens {
			log.Printf("BotToken[%d]: %s", idx+1, token)
		}
		return
	}

	// 2. 退出时的资源清理（延迟执行）
	defer func() {
		if infos.File != nil {
			if err := infos.File.Close(); err != nil {
				log.Printf("关闭日志文件错误: %v", err)
			}
		}
		var wg sync.WaitGroup
		for idx, client := range infos.BotClients {
			if client == nil {
				continue
			}
			idxCopy := idx
			clientCopy := client
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := clientCopy.Disconnect(); err != nil {
					log.Printf("Bot[%d] 退出失败: %+v", idxCopy+1, err)
				}
			}()
		}
		if infos.UserClient != nil {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := infos.UserClient.Disconnect(); err != nil {
					log.Printf("UserBot 退出失败: %+v", err)
				}
			}()
		}
		for name, client := range infos.UserClients {
			if client == nil || client == infos.UserClient {
				continue
			}
			nameCopy := name
			clientCopy := client
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := clientCopy.Disconnect(); err != nil {
					log.Printf("UserBot(%s) 退出失败: %+v", nameCopy, err)
				}
			}()
		}
		wg.Wait()
	}()

	// 3. 校验关键配置参数
	if infos.Conf.AppID == 0 || infos.Conf.AppHash == "" {
		log.Panicf("配置文件缺少必要的参数: AppID、AppHash")
		return
	}

	onlyDownloadMode := len(infos.Conf.BotTokens) == 0
	downloadCtx, cancelDownloads := context.WithCancel(context.Background())
	defer cancelDownloads()
	if onlyDownloadMode {
		log.Printf("BotToken 未配置, 将跳过 Bot 监听和 HTTP 服务, 仅执行自动下载")
	}

	// 4. 启动 Bot 客户端（仅在 BotToken 存在时启用）
	if !onlyDownloadMode {
		if err = infos.startBot(); err != nil {
			return
		}
	}

	if onlyDownloadMode {
		if err = infos.initUserClientsForDownloadOnly(); err != nil {
			log.Printf("初始化 UserBot 失败: %+v", err)
			return
		}
		signal.Ignore(syscall.SIGPIPE)
		statusChan := make(chan os.Signal, 1)
		signal.Notify(statusChan, os.Interrupt, syscall.SIGTERM)
		go infos.startConfiguredDownloads(downloadCtx)
		status := <-statusChan
		log.Printf("收到信号: %v, 正在退出...", status)
		cancelDownloads()
		log.Printf("自动下载流程结束, 程序退出")
		return
	}

	// 5. 优先按 userBots 配置初始化多账号；若未配置则回退到旧的单 UserBot 模式
	if len(infos.Conf.EffectiveUserBots()) > 0 {
		if err = infos.initUserClientsForDownloadOnly(); err != nil {
			log.Printf("初始化 UserBots 失败: %+v", err)
		} else if infos.UserClient != nil {
			infos.Status.Store(3)
			if infos.BotClient != nil {
				go infos.startConfiguredDownloads(downloadCtx)
			} else {
				infos.startConfiguredDownloads(downloadCtx)
			}
		}
	} else {
		// 兼容旧模式：初始化单 UserBot 客户端（此时只是连接, 尚未完成登录流程）
		if err = infos.userBotClient(); err != nil {
			log.Printf("UserBot 启动失败: %+v", err)
			return
		}

		// 检查 UserBot 登录状态, 尝试自动登录（若已存在 session）
		if err := infos.checkStatus(); err != nil {
			log.Printf("UserBot 登录失败: %+v", err)
			infos.resetStatus()
		}
	}

	// 忽略 SIGPIPE 信号, 防止由于网络异常断开导致进程崩溃
	signal.Ignore(syscall.SIGPIPE)

	// 设置系统中断信号监听, 用于优雅退出
	statusChan := make(chan os.Signal, 1)
	signal.Notify(statusChan, os.Interrupt, syscall.SIGTERM)

	var server *http.Server
	if infos.Conf.Port != 0 {
		// 创建 HTTP 服务器
		server = &http.Server{
			Addr:              fmt.Sprintf(":%d", infos.Conf.Port),
			Handler:           http.HandlerFunc(handleMain),
			ReadTimeout:       30 * time.Second,
			ReadHeaderTimeout: 10 * time.Second,
			IdleTimeout:       600 * time.Second,
			MaxHeaderBytes:    1 << 20, // 最大头部字节数 (1MB)
		}

		// 7. 在独立协程中启动 HTTP 服务
		go func() {
			log.Printf("HTTP 服务运行在 %d 端口", infos.Conf.Port)

			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Printf("HTTP 服务启动失败: %+v", err)
				statusChan <- os.Interrupt
			}
		}()
	} else {
		log.Printf("port=0，跳过 HTTP 服务监听")
	}

	// 8. 发送程序启动通知（优先发给 userID，未配置时默认发给第一个 Bot）
	sendMS(nil, "程序已启动 by jczhl", nil, 60)

	// 阻塞等待直到接收到退出信号
	status := <-statusChan
	log.Printf("收到信号: %v, 正在退出...", status)
	cancelDownloads()

	// 设置关闭的超时时间，例如 10 秒
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if server != nil {
		if err := server.Shutdown(ctx); err != nil {
			log.Printf("HTTP 服务关闭异常: %+v", err)
		} else {
			log.Printf("HTTP 服务已优雅关闭")
		}
	}
	sendMS(nil, "程序已退出 by jczhl", nil, 60)
}

// newInfos 初始化全局 Infos 对象, 加载日志和配置
func newInfos(filePath, filesPath string) (*Infos, error) {
	mutex := new(sync.RWMutex)
	infos := &Infos{
		FilePath:    filePath,
		FilesPath:   filesPath,
		Mutex:       mutex,
		Cond:        sync.NewCond(mutex),
		Code:        make(chan string, 1),
		Pass:        make(chan string, 1),
		BotIDs:      make(map[int64]struct{}, 2),
		HeadCache:   make(map[string]*MediaCache, 4),
		TailCache:   make(map[string]*MediaCache, 4),
		RelayInbox:  make(map[string]RelayInboxRecord, 16),
		UserClients: make(map[string]*telegram.Client, 2),
		UserClientIDs: make(map[string]int64, 2),
		Rex:         regexp.MustCompile(`(?i)(?:FLOOD(?:_PREMIUM)?_WAIT_(\d+)|WAIT(?:\s+OF)?\s*(\d+))`),
	}
	stdoutWriter := colorizedWriter{w: os.Stdout, prefix: "\x1b[32m", suffix: "\x1b[0m"}
	log.SetOutput(stdoutWriter)
	// 启动配置自动保存监听
	//go infos.watchConf()

	// 创建日志文件
	if filePath != "" {
		filePath = filepath.Clean(filePath)
		file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			log.Printf("无法打开日志文件: %v", err)
		}
		infos.File = file
		// 设置日志输出: 终端绿色, 文件保持纯文本
		if file != nil {
			log.SetOutput(io.MultiWriter(stdoutWriter, file))
		}
	}

	// 加载配置文件
	conf, err := loadConf(filesPath)
	if err != nil {
		log.Fatalf("载入配置文件失败: %+v", err)
	}
	if conf.Workers == 0 {
		conf.Workers = 1
	}
	if conf.MaxSize == 0 {
		conf.MaxSize = 32 * 1024 * 1024
	}
	infos.Conf = conf
	infos.IDs = make(map[int64]ID, len(conf.AdminIDs)+len(conf.WhiteIDs)+1)
	infos.buildIDs()

	for _, token := range conf.BotTokens {
		parts := strings.Split(token, ":")
		if len(parts) < 1 {
			return nil, fmt.Errorf("BotToken 格式错误: %s", token)
		}
		result := strings.TrimSpace(parts[0])
		botID, parseErr := strconv.ParseInt(result, 10, 64)
		if parseErr != nil {
			log.Printf("解析 BotID 失败: %+v", parseErr)
			continue
		}
		if infos.BotID == 0 {
			infos.BotID = botID
		}
		infos.BotIDs[botID] = struct{}{}
	}

	return infos, nil
}

// newOffSets 初始化全局翻页偏移量缓存
func newOffSets() *OffSets {
	return &OffSets{
		Mutex:   new(sync.Mutex),
		OffSets: make(map[string]OffSet),
	}
}

