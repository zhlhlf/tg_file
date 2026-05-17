package main

import (
	"fmt"
	"log"           // 用于日志记录
	"os"            // 用于文件操作
	"path/filepath" // 用于处理文件路径
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Conf 结构体定义了程序运行所需的各项配置参数
// 通过 yaml 标签与配置文件进行映射
type Conf struct {
	Site      string    `yaml:"site"`                // 反代域名, 用于生成公开访问链接
	AppHash   string    `yaml:"hash"`                // Telegram API Hash, 从 my.telegram.org 获取
	BotTokens []string  `yaml:"botTokens"`           // Telegram Bot Token 列表, 用于交互和管理
	Proxy     string    `yaml:"proxy,omitempty"`     // 代理服务器地址, 用于连接 Telegram
	Password  string    `yaml:"password,omitempty"`  // 访问 /link 接口时可选的身份验证密码
	Debug     bool      `yaml:"debug,omitempty"`     // 是否启用调试日志
	Channels  []string  `yaml:"channels,omitempty"`  // 频道列表, 用于搜索
	DC        int       `yaml:"dc,omitempty"`        // 指定连接的 Telegram 数据中心 (Data Center) ID
	Port      int       `yaml:"port"`                // 本地 HTTP 服务监听的端口
	Workers   int       `yaml:"workers,omitempty"`   // 文件下载/串流时的并发协程数
	AppID     int32     `yaml:"id"`                  // Telegram API ID, 从 my.telegram.org 获取
	MaxSize   int64     `yaml:"maxSize,omitempty"`   // 最大缓存大小
	UserID    int64     `yaml:"userID"`              // 管理员的 Telegram 用户 ID
	ChannelID int64     `yaml:"channelID,omitempty"` // 默认关联的频道 ID
	AdminIDs  []int64   `yaml:"adminIDs,omitempty"`  // 管理员 ID 列表, 拥有管理权限
	WhiteIDs  []int64   `yaml:"whiteIDs,omitempty"`  // 白名单 ID 列表, 允许使用部分功能
	Rules     []string  `yaml:"rules,omitempty"`     // 群管正则规则列表
	UserBots  []UserBot `yaml:"userBots,omitempty"`  // 多 UserBot 账号配置
	Download  Download  `yaml:"download,omitempty"`  // 自动下载任务配置
}

type UserBot struct {
	Name     string `yaml:"name"`
	Phone    string `yaml:"phone,omitempty"`
	Password string `yaml:"password,omitempty"`
	UserID   int64  `yaml:"userID,omitempty"`
	DC       int    `yaml:"dc,omitempty"`
}

type Download struct {
	Enabled     bool              `yaml:"enabled"`
	OutputDir   string            `yaml:"outputDir,omitempty"`
	PrivateChannel string         `yaml:"private_channel,omitempty"` // 已废弃保留字段；现改为 UserBot 直接转发到轮询 Bot 私聊后下载
	MaxCaptionLength int          `yaml:"max_caption_length,omitempty"` // 文件名中 caption 的最大长度，默认 90；小于等于 0 时也使用 90
	GlobalTypes []string          `yaml:"globalTypes,omitempty"`
	SkipNameContains []string     `yaml:"skipNameContains,omitempty"` // 最终文件名包含任一字符串时跳过下载
	Channels    []DownloadChannel `yaml:"channels,omitempty"`
	Concurrent  int               `yaml:"concurrent,omitempty"`  // 同时并发下载的频道数量限制, 0 表示不限制
	FileWorkers int               `yaml:"fileWorkers,omitempty"` // 每个文件内部的并发分片数, 0 表示使用全局 workers
	CacheItems  int               `yaml:"cacheItems,omitempty"`  // 媒体缓存最大条目数, 默认 10
	ScanInterval int              `yaml:"scanInterval,omitempty"` // 定时扫描间隔(秒), 0 表示不配置（代码默认 300s）
	ForceJoin   bool              `yaml:"forceJoin,omitempty"`   // 当账号未加入频道时尝试自动加入 (全局开关)
	Rclone      Rclone            `yaml:"rclone,omitempty"`      // rclone 远端存在性检查配置
}

type Rclone struct {
	Enabled      bool   `yaml:"enabled"`
	ConfigFile   string `yaml:"configFile,omitempty"`
	Remote       string `yaml:"remote,omitempty"`
	TransferMode string `yaml:"transferMode,omitempty"` // move 或 copy, 默认 move
}

type DownloadChannel struct {
	ID            int64    `yaml:"id"`
	FromMessageID int32    `yaml:"fromMessageID"`
	User          string   `yaml:"user,omitempty"`
	Join          string   `yaml:"join,omitempty"`
	Types         []string `yaml:"types,omitempty"`
	ForceJoin     bool     `yaml:"forceJoin,omitempty"`
}

type confRaw struct {
	Site      string       `yaml:"site"`
	AppHash   string       `yaml:"hash"`
	BotTokens any          `yaml:"botTokens"`
	Proxy     string       `yaml:"proxy,omitempty"`
	Password  string       `yaml:"password,omitempty"`
	Debug     bool         `yaml:"debug,omitempty"`
	Channels  []string     `yaml:"channels,omitempty"`
	DC        any          `yaml:"dc,omitempty"`
	Port      any          `yaml:"port"`
	Workers   any          `yaml:"workers,omitempty"`
	AppID     any          `yaml:"id"`
	MaxSize   any          `yaml:"maxSize,omitempty"`
	UserID    any          `yaml:"userID"`
	ChannelID any          `yaml:"channelID,omitempty"`
	AdminIDs  []any        `yaml:"adminIDs,omitempty"`
	WhiteIDs  []any        `yaml:"whiteIDs,omitempty"`
	Rules     []string     `yaml:"rules,omitempty"`
	UserBots  []userBotRaw `yaml:"userBots,omitempty"`
	Download  Download     `yaml:"download,omitempty"`
}

type userBotRaw struct {
	Name     string `yaml:"name"`
	Phone    string `yaml:"phone,omitempty"`
	Password string `yaml:"password,omitempty"`
	UserID   any    `yaml:"userID,omitempty"`
	DC       any    `yaml:"dc,omitempty"`
}

func (conf *Conf) UnmarshalYAML(value *yaml.Node) error {
	var raw confRaw
	if err := value.Decode(&raw); err != nil {
		return err
	}

	appID, err := parseInt32Field(raw.AppID, "id")
	if err != nil {
		return err
	}
	dc, err := parseIntField(raw.DC, "dc")
	if err != nil {
		return err
	}
	port, err := parseIntField(raw.Port, "port")
	if err != nil {
		return err
	}
	workers, err := parseIntField(raw.Workers, "workers")
	if err != nil {
		return err
	}
	maxSize, err := parseInt64Field(raw.MaxSize, "maxSize")
	if err != nil {
		return err
	}
	userID, err := parseInt64Field(raw.UserID, "userID")
	if err != nil {
		return err
	}
	channelID, err := parseInt64Field(raw.ChannelID, "channelID")
	if err != nil {
		return err
	}
	adminIDs, err := parseInt64SliceField(raw.AdminIDs, "adminIDs")
	if err != nil {
		return err
	}
	whiteIDs, err := parseInt64SliceField(raw.WhiteIDs, "whiteIDs")
	if err != nil {
		return err
	}
	userBots, err := parseUserBots(raw.UserBots)
	if err != nil {
		return err
	}
	botTokens, err := parseStringSliceField(raw.BotTokens, "botTokens")
	if err != nil {
		return err
	}

	*conf = Conf{
		Site:      raw.Site,
		AppHash:   raw.AppHash,
		BotTokens: botTokens,
		Proxy:     raw.Proxy,
		Password:  raw.Password,
		Debug:     raw.Debug,
		Channels:  raw.Channels,
		DC:        dc,
		Port:      port,
		Workers:   workers,
		AppID:     appID,
		MaxSize:   maxSize,
		UserID:    userID,
		ChannelID: channelID,
		AdminIDs:  adminIDs,
		WhiteIDs:  whiteIDs,
		Rules:     raw.Rules,
		UserBots:  userBots,
		Download:  raw.Download,
	}
	return nil
}

func parseStringSliceField(v any, field string) ([]string, error) {
	if v == nil {
		return nil, nil
	}
	switch value := v.(type) {
	case string:
		src := strings.TrimSpace(value)
		if src == "" {
			return nil, nil
		}
		return []string{src}, nil
	case []any:
		result := make([]string, 0, len(value))
		for idx, item := range value {
			str, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("字段 %s[%d] 类型 %T 不支持，需为字符串", field, idx, item)
			}
			str = strings.TrimSpace(str)
			if str != "" {
				result = append(result, str)
			}
		}
		if len(result) == 0 {
			return nil, nil
		}
		return result, nil
	default:
		return nil, fmt.Errorf("字段 %s 类型 %T 不支持，需为字符串或字符串数组", field, v)
	}
}

type downloadChannelRaw struct {
	ID            any      `yaml:"id"`
	FromMessageID any      `yaml:"fromMessageID"`
	User          string   `yaml:"user,omitempty"`
	Join          string   `yaml:"join,omitempty"`
	Types         []string `yaml:"types,omitempty"`
}

func (ch *DownloadChannel) UnmarshalYAML(value *yaml.Node) error {
	var raw downloadChannelRaw
	if err := value.Decode(&raw); err != nil {
		return err
	}

	id, err := parseInt64Field(raw.ID, "download.channels.id")
	if err != nil {
		return err
	}
	fromID, err := parseInt32Field(raw.FromMessageID, "download.channels.fromMessageID")
	if err != nil {
		return err
	}

	ch.ID = id
	ch.FromMessageID = fromID
	ch.User = strings.TrimSpace(raw.User)
	ch.Join = strings.TrimSpace(raw.Join)
	ch.Types = raw.Types
	return nil
}

func parseUserBots(raws []userBotRaw) ([]UserBot, error) {
	if len(raws) == 0 {
		return nil, nil
	}
	bots := make([]UserBot, 0, len(raws))
	for idx, raw := range raws {
		name := strings.TrimSpace(raw.Name)
		if name == "" {
			name = fmt.Sprintf("user%d", idx+1)
		}
		uid, err := parseInt64Field(raw.UserID, fmt.Sprintf("userBots[%d].userID", idx))
		if err != nil {
			return nil, err
		}
		dc, err := parseIntField(raw.DC, fmt.Sprintf("userBots[%d].dc", idx))
		if err != nil {
			return nil, err
		}
		bots = append(bots, UserBot{
			Name:     name,
			Phone:    strings.TrimSpace(raw.Phone),
			Password: raw.Password,
			UserID:   uid,
			DC:       dc,
		})
	}
	return bots, nil
}

func (conf *Conf) EffectiveUserBots() []UserBot {
	if len(conf.UserBots) > 0 {
		return conf.UserBots
	}
	return []UserBot{{
		Name:   "default",
		UserID: conf.UserID,
		DC:     conf.DC,
	}}
}

func (conf *Conf) EffectiveDownloadUserBots() []UserBot {
	return conf.EffectiveUserBots()
}

func parseIntField(v any, field string) (int, error) {
	if v == nil {
		return 0, nil
	}
	parsed, err := parseInt64Any(v, field)
	if err != nil {
		return 0, err
	}
	return int(parsed), nil
}

func parseInt32Field(v any, field string) (int32, error) {
	if v == nil {
		return 0, nil
	}
	parsed, err := parseInt64Any(v, field)
	if err != nil {
		return 0, err
	}
	return int32(parsed), nil
}

func parseInt64Field(v any, field string) (int64, error) {
	if v == nil {
		return 0, nil
	}
	return parseInt64Any(v, field)
}

func parseInt64SliceField(values []any, field string) ([]int64, error) {
	if len(values) == 0 {
		return nil, nil
	}
	result := make([]int64, 0, len(values))
	for idx, value := range values {
		parsed, err := parseInt64Any(value, fmt.Sprintf("%s[%d]", field, idx))
		if err != nil {
			return nil, err
		}
		result = append(result, parsed)
	}
	return result, nil
}

func parseInt64Any(v any, field string) (int64, error) {
	switch value := v.(type) {
	case int:
		return int64(value), nil
	case int8:
		return int64(value), nil
	case int16:
		return int64(value), nil
	case int32:
		return int64(value), nil
	case int64:
		return value, nil
	case uint:
		return int64(value), nil
	case uint8:
		return int64(value), nil
	case uint16:
		return int64(value), nil
	case uint32:
		return int64(value), nil
	case uint64:
		return int64(value), nil
	case string:
		src := strings.TrimSpace(value)
		if src == "" {
			return 0, nil
		}
		parsed, err := strconv.ParseInt(src, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("字段 %s 值 %q 不是有效整数", field, value)
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("字段 %s 类型 %T 不支持，需为数字或字符串数字", field, v)
	}
}

// loadConf 从指定路径加载 config.yaml 配置文件
// 如果文件不存在或解析失败, 将返回错误
func loadConf(filesPath string) (*Conf, error) {
	yamlPath := filepath.Join(filesPath, "config.yaml")

	bytes, err := os.ReadFile(yamlPath)
	if err != nil {
		log.Printf("读取 config.yaml 文件错误: %+v", err)
		return nil, err
	}

	var conf Conf
	if err := yaml.Unmarshal(bytes, &conf); err != nil {
		log.Printf("解析 config.yaml 文件错误: %+v", err)
		return nil, err
	}

	if conf.Download.OutputDir == "" {
		conf.Download.OutputDir = "downloads"
	}

	return &conf, nil // 返回解析后的配置对象
}

// saveConf 将当前的配置信息序列化并保存到 config.yaml 文件中
// 常用于在程序运行过程中动态更新配置（如通过 Bot 命令添加白名单）
func saveConf(conf *Conf, filesPath string) error {
	configPath := filepath.Join(filesPath, "config.yaml")

	bytes, err := yaml.Marshal(conf)
	if err != nil {
		log.Printf("序列化 config.yaml 文件错误: %+v", err)
		return err
	}

	// 将字节数组写入到配置文件并返回结果
	if err := os.WriteFile(configPath, bytes, 0644); err != nil {
		log.Printf("写入 config.yaml 文件错误: %+v", err)
		return err
	}
	return nil
}
