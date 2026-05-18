package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

var rcloneRemoteDirNotFoundText = regexp.MustCompile(`(?i)(directory not found|couldn't find directory|not found)`)

type rcloneListEntry struct {
	Name  string `json:"Name"`
	IsDir bool   `json:"IsDir"`
}

func (infos *Infos) rcloneRemotePath(outputRoot, finalPath string) (string, error) {
	if infos == nil || infos.Conf == nil {
		return "", nil
	}
	rcloneConf := infos.Conf.Download.Rclone
	remoteRoot := strings.TrimSpace(rcloneConf.Remote)
	if remoteRoot == "" {
		return "", fmt.Errorf("rclone 已启用但未配置 remote")
	}
	relPath, err := filepath.Rel(outputRoot, finalPath)
	if err != nil {
		return "", err
	}
	return joinRclonePath(remoteRoot, filepath.ToSlash(relPath)), nil
}

func (infos *Infos) rcloneFileExists(ctx context.Context, outputRoot, finalPath string) (bool, string, error) {
	if infos == nil || infos.Conf == nil {
		return false, "", nil
	}
	rcloneConf := infos.Conf.Download.Rclone
	if !rcloneConf.Enabled {
		return false, "", nil
	}
	remoteRoot := strings.TrimSpace(rcloneConf.Remote)
	if remoteRoot == "" {
		return false, "", fmt.Errorf("rclone 已启用但未配置 remote")
	}
	relPath, err := filepath.Rel(outputRoot, finalPath)
	if err != nil {
		return false, "", err
	}
	remotePath := joinRclonePath(remoteRoot, filepath.ToSlash(relPath))
	remoteDir, remoteName := splitRclonePath(remotePath)
	if remoteName == "" {
		return false, "", nil
	}

	files, err := infos.rcloneListDirFiles(ctx, remoteDir)
	if err == nil {
		if _, ok := files[remoteName]; ok {
			return true, "精确", nil
		}
		if rcloneConf.FuzzyMatchID && rcloneNameMatchesMessageID(remoteName, files) {
			return true, "模糊ID", nil
		}
		return false, "", nil
	}
	debugf("rclone目录缓存检查失败，回退单文件检查: dir=%s file=%s err=%v", remoteDir, remoteName, err)

	args := infos.rcloneArgs("lsjson", "--stat", remotePath)
	cmd := exec.CommandContext(ctx, "rclone", args...)
	if output, err := cmd.CombinedOutput(); err != nil {
		text := strings.TrimSpace(string(output))
		if rcloneRemoteDirNotFoundText.MatchString(text) {
			return false, "", nil
		}
		if text != "" {
			return false, "", fmt.Errorf("%w: %s", err, text)
		}
		return false, "", err
	}
	return true, "精确", nil
}

func (infos *Infos) rcloneListDirFiles(ctx context.Context, remoteDir string) (map[string]struct{}, error) {
	remoteDir = strings.TrimSpace(remoteDir)
	if remoteDir == "" {
		return nil, fmt.Errorf("rclone 目录为空")
	}

	infos.Mutex.RLock()
	if infos.RcloneDirCache != nil {
		if entry, ok := infos.RcloneDirCache[remoteDir]; ok {
			files := entry.Files
			infos.Mutex.RUnlock()
			return files, nil
		}
	}
	infos.Mutex.RUnlock()

	args := infos.rcloneArgs("lsjson", "--files-only", remoteDir)
	cmd := exec.CommandContext(ctx, "rclone", args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		text := strings.TrimSpace(string(output))
		if rcloneRemoteDirNotFoundText.MatchString(text) {
			files := map[string]struct{}{}
			infos.storeRcloneDirCache(remoteDir, files)
			return files, nil
		}
		if text != "" {
			return nil, fmt.Errorf("%w: %s", err, text)
		}
		return nil, err
	}

	var entries []rcloneListEntry
	if err := json.Unmarshal(output, &entries); err != nil {
		return nil, err
	}
	files := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		name := strings.TrimSpace(entry.Name)
		if name == "" || entry.IsDir {
			continue
		}
		files[name] = struct{}{}
	}
	infos.storeRcloneDirCache(remoteDir, files)
	return files, nil
}

func (infos *Infos) storeRcloneDirCache(remoteDir string, files map[string]struct{}) {
	if infos == nil || infos.Mutex == nil {
		return
	}
	infos.Mutex.Lock()
	// 只保留当前检查目录的缓存：当扫描到不同目录时，旧目录缓存立即失效。
	infos.RcloneDirCache = map[string]rcloneDirCacheEntry{
		remoteDir: {Files: files},
	}
	infos.Mutex.Unlock()
}

func (infos *Infos) rcloneMoveFile(ctx context.Context, localPath, remotePath string) error {
	if infos == nil || infos.Conf == nil {
		return nil
	}
	rcloneConf := infos.Conf.Download.Rclone
	if !rcloneConf.Enabled {
		return nil
	}
	args := infos.rcloneArgs("moveto", localPath, remotePath)
	cmd := exec.CommandContext(ctx, "rclone", args...)
	if output, err := cmd.CombinedOutput(); err != nil {
		text := strings.TrimSpace(string(output))
		if text != "" {
			return fmt.Errorf("%w: %s", err, text)
		}
		return err
	}
	return nil
}

func (infos *Infos) rcloneTransferFile(ctx context.Context, localPath, remotePath, mode string) error {
	debugf("rclone传输: mode=%s local=%s remote=%s", strings.ToLower(strings.TrimSpace(mode)), localPath, remotePath)
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "copy":
		return infos.rcloneCopyFile(ctx, localPath, remotePath)
	default:
		return infos.rcloneMoveFile(ctx, localPath, remotePath)
	}
}

func (infos *Infos) rcloneCopyFile(ctx context.Context, localPath, remotePath string) error {
	if infos == nil || infos.Conf == nil {
		return nil
	}
	rcloneConf := infos.Conf.Download.Rclone
	if !rcloneConf.Enabled {
		return nil
	}
	args := infos.rcloneArgs("copyto", localPath, remotePath)
	cmd := exec.CommandContext(ctx, "rclone", args...)
	if output, err := cmd.CombinedOutput(); err != nil {
		text := strings.TrimSpace(string(output))
		if text != "" {
			return fmt.Errorf("%w: %s", err, text)
		}
		return err
	}
	return nil
}

func (infos *Infos) rcloneArgs(extra ...string) []string {
	args := make([]string, 0, len(extra)+2)
	rcloneConf := infos.Conf.Download.Rclone
	configFile := strings.TrimSpace(rcloneConf.ConfigFile)
	if configFile != "" {
		args = append(args, "--config", configFile)
	}
	args = append(args, extra...)
	return args
}

func (infos *Infos) rcloneTransferMode() string {
	if infos == nil || infos.Conf == nil {
		return "move"
	}
	mode := strings.ToLower(strings.TrimSpace(infos.Conf.Download.Rclone.TransferMode))
	if mode == "copy" {
		return "copy"
	}
	return "move"
}

func joinRclonePath(base, rel string) string {
	base = strings.TrimSpace(base)
	rel = strings.TrimLeft(strings.TrimSpace(rel), "/")
	if base == "" {
		return rel
	}
	if rel == "" {
		return base
	}
	if strings.HasSuffix(base, ":") {
		return base + rel
	}
	return strings.TrimRight(base, "/") + "/" + rel
}

func splitRclonePath(remotePath string) (string, string) {
	remotePath = strings.TrimRight(strings.TrimSpace(remotePath), "/")
	if remotePath == "" {
		return "", ""
	}
	idx := strings.LastIndex(remotePath, "/")
	if idx == -1 {
		if strings.HasSuffix(remotePath, ":") {
			return remotePath, ""
		}
		return "", remotePath
	}
	return remotePath[:idx], remotePath[idx+1:]
}

func rcloneNameMatchesMessageID(targetName string, files map[string]struct{}) bool {
	msgID := rcloneMessageID(targetName)
	if msgID == "" {
		return false
	}
	for name := range files {
		candidateID := rcloneMessageID(name)
		if candidateID == msgID {
			debugf("rclone模糊匹配命中: target=%s exists=%s", targetName, name)
			return true
		}
	}
	return false
}

func rcloneMessageID(name string) string {
	base := filepath.Base(strings.TrimSpace(name))
	stem := strings.TrimSuffix(base, filepath.Ext(base))
	idx := 0
	for idx < len(stem) && stem[idx] >= '0' && stem[idx] <= '9' {
		idx++
	}
	if idx == 0 {
		return ""
	}
	return stem[:idx]
}
