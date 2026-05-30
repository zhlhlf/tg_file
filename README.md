# TGFileBot

TGFileBot 是一个基于 Go 的 Telegram 频道媒体自动下载工具。它可以使用多个 UserBot 账号读取频道消息，支持多个 Bot 参与分流下载，并可在下载完成后通过 rclone 转存到远端存储。

## 主要功能

- 自动下载 Telegram 频道历史消息和新增消息中的媒体文件
- 支持多 UserBot 账号登录、轮询和指定账号下载
- 支持多个 Bot 登录，第一个 Bot 用于管理，其余 Bot 可参与分流下载
- 支持通过频道链接自动解析频道 ID
- 支持账号未加入频道时自动尝试加入
- 支持按媒体类型过滤：`video`、`photo`、`document`、`all`
- 支持按文件名关键词跳过下载
- 支持下载停滞检测，临时文件 20 秒无变化会取消并交由上层重试
- 支持下载完成后的文件大小校验
- 支持按频道名和年月自动整理下载目录
- 支持 rclone 检查远端文件是否已存在，并在下载后 `move` 或 `copy` 到远端
- 支持 Bot 私聊管理命令、白名单、日志查询、代理和并发配置
- 支持通过 `@BotFather` 批量创建 Bot，并自动写回 token

## 工作方式

程序启动后会读取 `files/config.yaml`：

1. 初始化配置、日志和 session 目录。
2. 登录配置中的 Bot；如果没有配置 BotToken，则进入仅下载模式。
3. 初始化一个或多个 UserBot 账号。
4. 扫描 `download.channels` 中配置的频道。
5. 从 `fromMessageID` 开始批量读取消息。
6. 过滤出符合类型的媒体消息。
7. 下载到本地临时目录，校验大小后移动到最终目录。
8. 如果启用 rclone，则检查远端是否存在，并在下载完成后转存。
9. 初次下载完成后按 `scanInterval` 定时扫描新增消息。

## 目录结构

- `main.go`：程序入口、启动参数、运行模式、退出清理
- `config.go`：配置文件结构和 YAML 解析
- `client.go`：Bot / UserBot 初始化、登录、session 加载
- `command.go`：Bot 私聊命令处理
- `download.go`：频道扫描、消息拉取、并发下载调度、增量扫描
- `download_dict.go`：单个媒体文件下载、临时文件、大小校验、停滞看门狗
- `download_relay.go`：UserBot 到 Bot 的分流下载逻辑
- `download_rclone.go`：rclone 远端存在性检查和上传逻辑
- `media_target.go`：下载文件名、caption、频道目录和年月目录生成
- `bot_factory.go`：通过 BotFather 批量创建 Bot / 收集 BotToken
- `auth.go`：管理员、白名单和内部 UserBot 权限判断
- `logger.go`：调试日志和错误日志封装
- `util.go`：日志读取、临时目录清理等辅助函数
- `files/config.yaml.example`：配置示例

## 快速开始

### 1. 复制配置文件

```powershell
copy files\config.yaml.example files\config.yaml
```

### 2. 修改配置

打开 `files/config.yaml`，至少需要填写：

- `id`：Telegram API ID
- `hash`：Telegram API Hash
- `userBots`：至少一个 Telegram 用户账号
- `download.channels`：需要下载的频道

如果需要 Bot 管理或 Bot 分流下载，再填写 `botTokens`。

### 3. 启动程序

```powershell
go run . -files files
```

或运行已编译好的程序：

```powershell
.\tgfilebot.exe -files files
```

## 启动参数

- `-files`：配置目录，默认 `files`
- `-log`：日志文件路径，不传则只输出到终端
- `-version` / `-v`：输出版本号并退出

示例：

```powershell
.\tgfilebot.exe -files files -log files\run.log
```

## 运行模式

### 仅下载模式

当 `botTokens` 为空时，程序不会启动 Bot 监听，只初始化 UserBot 并执行自动下载。

```yaml
botTokens: []
```

### Bot + UserBot 协作模式

当配置了 `botTokens` 时：

- 所有 Bot 会登录；
- 第一个 Bot 用于私聊管理；
- 所有可用 Bot 可参与分流下载；
- UserBot 负责读取频道、解析消息和转发媒体。

## 配置说明

主要配置文件是 `files/config.yaml`。

### 基础配置

```yaml
id: 0
hash: ""
botTokens:
  - "123456:AAAA_BOT_TOKEN_1"
proxy: ""
debug: false
workers: 1
adminIDs: []
whiteIDs: []
```

字段说明：

- `id`：Telegram API ID
- `hash`：Telegram API Hash
- `botTokens`：Bot Token 列表；可以为空，为空时只执行下载
- `proxy`：代理地址，例如 `socks5://127.0.0.1:7890`
- `debug`：是否开启调试日志
- `workers`：默认下载线程数
- `adminIDs`：管理员 ID 列表
- `whiteIDs`：允许使用 Bot 的白名单 ID 列表

### UserBot 配置

```yaml
userBots:
  - phone: "+8613800000000"
    password: "2FA密码，可选"
    userID: 123456789
    dc: 4
  - phone: "+8613900000000"
```

说明：

- 支持多个 UserBot。
- `phone` 建议带国家码。
- `password` 是 Telegram 二步验证密码，可选。
- `userID` 可用于校验登录账号是否正确。
- `dc` 可指定该账号连接的数据中心。

### 下载配置

```yaml
download:
  enabled: true
  outputDir: downloads
  max_caption_length: 90
  scanInterval: 300
  globalTypes:
    - video
    - photo
  skipNameContains:
    - "广告"
  requireNameContains:
    - "关键词"
  maxSize: 0
  concurrent: 2
  fileWorkers: 4
  batchSize: 100
  batchDelay: 4
  forceJoin: true
  channels:
    - join: "https://t.me/channelname"
      fromMessageID: 1
      requireNameContains:
        - "关键词A"
      maxSize: 1GB
      types:
        - video
    - id: -1001234567890
      fromMessageID: 1
      user: user1
      requireNameContains:
        - "关键词B"
      maxSize: 500MB
      types:
        - video
        - photo
        - document
```

字段说明：

- `download.enabled`：是否启用自动下载
- `download.outputDir`：下载输出目录
- `download.max_caption_length`：文件名中 caption 的最大长度，默认 `90`
- `download.scanInterval`：增量扫描间隔，单位秒；小于等于 0 时当前代码默认使用 `300`
- `download.globalTypes`：全局媒体类型过滤
- `download.skipNameContains`：文件名包含指定关键词时跳过下载
- `download.requireNameContains`：不为空时，文件名必须包含任一关键词才下载
- `download.maxSize`：全局文件大小上限，`0` 或不配置表示不限制；支持纯字节数或 `KB` / `MB` / `GB` / `TB`
- `download.concurrent`：同时执行的文件下载任务数，默认 `3`
- `download.fileWorkers`：单个文件内部下载线程数；小于等于 0 时使用 `workers`
- `download.batchSize`：每次批量拉取消息数量，默认 `100`
- `download.batchDelay`：下载队列低于阈值时，检查并补充下一批消息的间隔，单位秒；小于等于 `0` 时默认 `4`
- `download.forceJoin`：账号无法访问频道时是否尝试自动加入
- `download.channels`：需要下载的频道列表

频道字段说明：

- `id`：频道 ID，可选；如果只写 `join`，程序会尝试自动解析
- `join`：频道用户名链接或邀请链接
- `fromMessageID`：从哪条消息开始下载
- `user`：指定使用某个 UserBot；为空时自动选择
- `types`：该频道自己的媒体类型过滤，会覆盖 `globalTypes`
- `requireNameContains`：该频道自己的文件名必含过滤；不为空时覆盖 `download.requireNameContains`
- `maxSize`：该频道文件大小上限；大于 `0` 时覆盖 `download.maxSize`
- `forceJoin`：该频道是否允许自动加入

## rclone 远端转存

启用后，程序会在下载前检查远端是否已有同名文件；下载完成后按配置执行 `move` 或 `copy`。

```yaml
download:
  rclone:
    enabled: true
    configFile: C:/Users/Administrator/.config/rclone/rclone.conf
    transferMode: move
    remote: myremote:downloads
    checkRemote:
      - myremote:old_downloads
      - myremote:backup_downloads
```

字段说明：

- `enabled`：是否启用 rclone
- `configFile`：rclone 配置文件路径，可选
- `remote`：远端根路径，例如 `myremote:downloads`
- `checkRemote`：远端存在性检查根路径列表，可选；为空列表时使用 `remote`；配置多个源时下载前会逐个检查，上传/转存仍使用 `remote`
- `transferMode`：`move` 或 `copy`，默认 `move`

需要确保系统环境中可以直接执行 `rclone` 命令。

## 下载文件命名规则

最终文件大致保存为：

```text
downloads/
  频道名/
    2026_05/
      12345 - caption内容.mp4
```

规则：

- 按频道名建立目录；
- 按消息时间建立 `YYYY_MM` 目录；
- 文件名以消息 ID 开头；
- 有 caption 时会加入文件名；
- 会自动替换 Windows 不允许的文件名字符；
- 如果同名文件已存在，会跳过下载。

## Bot 私聊命令

主 Bot 支持以下常用命令：

- `/start`：查看状态
- `/qr`：使用二维码登录 UserBot
- `/phone 手机号`：发起手机号登录
- `/code 验证码`：提交验证码
- `/pass 密码`：提交 2FA 密码
- `/allow 用户ID`：添加白名单
- `/disallow 用户ID`：移除白名单
- `/list`：查看白名单
- `/dc`：查看或设置默认 DC
- `/proxy`：查看或设置代理，`/proxy off` 可关闭
- `/workers`：查看或设置默认并发数
- `/info`：查看日志，可带关键词和行数
- `/makebots`：使用第一个 UserBot 创建 5 个 Bot

## Bot 批量创建

命令行方式：

```powershell
.\tgfilebot.exe makebots
```

默认创建 `5` 个 Bot。也可以指定数量：

```powershell
.\tgfilebot.exe makebots 10
```

收集当前 UserBot 账号下已有 Bot 的 token：

```powershell
.\tgfilebot.exe makebots get
```

说明：

- 使用配置中第一个 UserBot 与 `@BotFather` 交互；
- 创建前会尝试发送 `/cancel` 重置 BotFather 状态；
- 创建成功后会把 token 写入 `files/config.yaml`；
- 创建多个 Bot 时每个之间会等待一段时间，避免触发限制。

## 构建

普通构建：

```powershell
go build -buildvcs=false
```

Windows amd64：

```powershell
$env:GOOS = "windows"
$env:GOARCH = "amd64"
$env:CGO_ENABLED = "0"
go build -buildvcs=false -trimpath -ldflags "-s -w" -o dist/tgfilebot-windows-amd64.exe .
```

Linux amd64：

```powershell
$env:GOOS = "linux"
$env:GOARCH = "amd64"
$env:CGO_ENABLED = "0"
go build -buildvcs=false -trimpath -ldflags "-s -w" -o dist/tgfilebot-linux-amd64 .
```

## 运行提示

- 首次登录 UserBot 可能需要输入验证码或 2FA 密码。
- session 文件保存在 `files/sessions/`，请妥善保管。
- `botTokens`、手机号、session 都是敏感信息，不建议提交到公开仓库。
- 下载并发不宜过高，过高可能触发 Telegram 限流或导致下载变慢。
- 如果启用 Bot 分流下载，建议配置多个 BotToken 分散压力。
- 如果启用 rclone，请先确认 `rclone lsjson`、`rclone moveto` 或 `rclone copyto` 在当前环境可正常执行。

## 许可证

本项目使用 `LICENSE` 中声明的许可证。
