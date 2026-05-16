package main

import (
	"context"
	"fmt"
	"path/filepath"
	"log"
	"strings"
	"time"
)

func buildMediaTargetPath(outputRoot, channelName string, msgTime time.Time, fileName string) string {
	channelName = sanitizeFileName(strings.TrimSpace(channelName))
	return filepath.Join(outputRoot, channelName, fmt.Sprintf("%04d_%02d", msgTime.Year(), msgTime.Month()), fileName)
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
