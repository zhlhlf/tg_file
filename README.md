# TGFileBot

TGFileBot 是一个基于 Go 的 Telegram 文件浏览、直链解析和自动下载工具。它支持 Bot 和多个 UserBot 账号协作，提供频道搜索、文件流式访问、自动下载、强制入群和发布构建等能力。

## 功能

- Telegram 频道媒体搜索和直链解析
- 文件流式下载，支持 Range 请求
- 自动下载频道文件
- 多 UserBot 账号支持
- 频道未加入时可自动尝试加入
- 下载完成后进行文件大小和 MD5 校验
- 支持 HTTP 服务与可选 Bot 交互
- GitHub Actions 手动发布 Linux / Windows 版本

## 目录结构

- `main.go`：程序入口
- `config.go`：配置解析
- `client.go`：Telegram 客户端和登录逻辑
- `download.go`：自动下载逻辑
- `stream.go`：流式下载逻辑
- `http.go`：HTTP 接口
- `util.go`：辅助函数
- `files/config.yaml.example`：配置示例

## 快速开始

1. 复制示例配置文件。

```powershell
copy files\config.yaml.example files\config.yaml
```

2. 修改 `files/config.yaml`，填入 Telegram `id`、`hash`、`userBots`、`botTokens` 等信息。

```yaml
botTokens:
  - "123456:AAAA_BOT_TOKEN_1"
  - "789012:BBBB_BOT_TOKEN_2"
```

3. 启动程序。

```powershell
go run . -files files
```

如果你只想运行已经编译好的程序，可以直接启动生成的二进制。

```powershell
.\tgfilebot.exe -files files
```

## 启动参数

- `-files`：配置目录，默认是 `files`
- `-log`：日志文件路径
- `-version` / `-v`：输出版本号并退出

## Bot 批量创建

程序支持使用 `userBots` 中的第一个账号，通过 `@BotFather` 批量创建机器人，并将新 token 自动写回 `files/config.yaml` 的 `botTokens`。

### 命令行方式

```powershell
.\tgfilebot.exe makebots
```

- 默认创建 `5` 个 bot

```powershell
.\tgfilebot.exe makebots 10
```

- 可指定创建数量，参数必须为正整数

### 机器人命名规则

- 用户名格式：`英文数字10位_bot`
- 例如：`a1b2c3d4e5_bot`

### 执行逻辑

- 使用 `userBots` 里的第一个账号
- 按 `BotFather` 的标准流程执行：
  1. 发送 `/newbot`
  2. 等待输入名称提示
  3. 发送 bot 名称
  4. 等待输入用户名提示
  5. 发送 bot 用户名
  6. 从成功消息中提取 token
- 每创建一个 bot 后休眠 `2` 秒
- 当出现 `too many attempts` 限流提示时，会自动等待指定秒数后继续

### 注意事项

- 需要确保 `userBots` 第一个账号已经成功登录
- 如果 `BotFather` 会话被打断，程序会在每次创建前先尝试 `/cancel` 重置状态
- 创建成功后的 token 会自动追加到 `files/config.yaml`

## 配置说明

主要配置文件是 `files/config.yaml`。下面是常见字段说明：

- `id`：Telegram API ID
- `hash`：Telegram API Hash
- `botTokens`：Bot Token，支持单个字符串或字符串列表；推荐写成 YAML 列表，一行一个 token；列表模式下启动时会登录全部 Bot，第一个 Bot 作为主通知 Bot
- `site`：站点域名，用于链接生成
- `port`：HTTP 服务端口，默认 `0`，仅当设置为非 `0` 数字时才会监听 HTTP
- `password`：访问接口时使用的密码
- `debug`：是否开启调试日志
- `workers`：下载和流式读取的基础并发数
- `maxSize`：缓存阈值
- `userID`：管理员用户 ID
- `userBots`：多个 UserBot 账号配置
- `download.enabled`：是否启用自动下载
- `download.outputDir`：下载输出目录
- `download.private_channel`：私有中转频道 URL，配置后会先由 UserBot 转发到该频道，再由多个 Bot 轮询下载
- `download.globalTypes`：默认允许下载的媒体类型
- `download.concurrent`：同时处理的频道数量
- `download.fileWorkers`：单文件内部并发分片数
- `download.forceJoin`：未加入频道时是否自动尝试加入
- `download.rclone.enabled`：是否启用 rclone 远端存在性检查和上传 move
- `download.rclone.configFile`：指定 rclone 配置文件路径，等价于命令行 `--config`
- `download.rclone.remote`：rclone 远端根路径，例如 `myremote:downloads`
- `download.rclone.transferMode`：上传方式，支持 `move` 或 `copy`，默认 `move`
- `download.channels`：自动下载的频道列表

### 频道下载配置示例

```yaml
download:
  enabled: true
  outputDir: downloads
  private_channel: "https://t.me/your_private_channel"
  concurrent: 2
  fileWorkers: 4
  forceJoin: true
  rclone:
    enabled: true
    configFile: C:/Users/Administrator/.config/rclone/rclone.conf
    transferMode: move
    remote: myremote:downloads
  channels:
    - id: -1001234567890
      fromMessageID: 1
      user: user1
      join: t.me/your_channel
      types:
        - video
        - photo
```

### UserBot 示例

```yaml
userBots:
  - name: user1
    phone: "+8613800000000"
  - name: user2
    phone: "+8613900000000"
```

## 构建

由于当前仓库环境可能没有完整的 VCS 信息，建议构建时显式关闭 VCS stamping。

```powershell
go build -buildvcs=false
```

如果你想生成指定平台版本，可以手动设置环境变量：

```powershell
$env:GOOS = "linux"
$env:GOARCH = "amd64"
$env:CGO_ENABLED = "0"
go build -buildvcs=false -trimpath -ldflags "-s -w" -o dist/tgfilebot-linux-amd64 .
```

```powershell
$env:GOOS = "windows"
$env:GOARCH = "amd64"
$env:CGO_ENABLED = "0"
go build -buildvcs=false -trimpath -ldflags "-s -w" -o dist/tgfilebot-windows-amd64.exe .
```

## GitHub Actions 自动/手动发布

仓库已添加手动触发的发布工作流：`.github/workflows/release.yml`。

它会：

- push 到 `main` 分支时自动触发
- 也支持在 GitHub Actions 中手动触发
- 使用当前提交的 `github.sha` 作为 release tag 和名称
- 同时构建 Linux amd64 和 Windows amd64 版本
- 自动上传构建产物到 GitHub Release

## 运行提示

- 首次登录 UserBot 时可能需要手动输入验证码或 2FA 密码
- 如果启用了自动下载，程序启动后会自动扫描配置中的频道
- 如果账号未加入频道且开启了 `download.forceJoin`，程序会尝试自动加入
- 如果开启了 `download.rclone.enabled`，程序会先检查 rclone 远端是否已存在同名文件，存在则直接跳过；远端路径会自动带上本地 `outputDir` 的目录名，然后按 `download.rclone.transferMode` 使用 rclone move 或 copy
- 下载完成后会校验文件大小，并在可用时校验 MD5

## 许可证

本项目使用 `LICENSE` 中声明的许可证。