package main

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
)

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

func (infos *Infos) rcloneFileExists(ctx context.Context, outputRoot, finalPath string) (bool, error) {
	if infos == nil || infos.Conf == nil {
		return false, nil
	}
	rcloneConf := infos.Conf.Download.Rclone
	if !rcloneConf.Enabled {
		return false, nil
	}
	remoteRoot := strings.TrimSpace(rcloneConf.Remote)
	if remoteRoot == "" {
		return false, fmt.Errorf("rclone 已启用但未配置 remote")
	}
	relPath, err := filepath.Rel(outputRoot, finalPath)
	if err != nil {
		return false, err
	}
	remotePath := joinRclonePath(remoteRoot, filepath.ToSlash(relPath))
	args := infos.rcloneArgs("lsjson", "--stat", remotePath)
	cmd := exec.CommandContext(ctx, "rclone", args...)
	if output, err := cmd.CombinedOutput(); err != nil {
		text := strings.TrimSpace(string(output))
		if strings.Contains(strings.ToLower(text), "directory not found") {
			return false, nil
		}
		if text != "" {
			return false, fmt.Errorf("%w: %s", err, text)
		}
		return false, err
	}
	return true, nil
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