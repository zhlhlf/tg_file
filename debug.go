package main

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
)

func debugf(format string, args ...any) {
	if infos == nil || infos.Conf == nil {
		return
	}
	if infos.Conf.Debug {
		log.Printf(format, args...)
	}
}

func humanizeBytes(b int64) string {
	if b < 1024 {
		return fmt.Sprintf("%dB", b)
	}
	kb := float64(b) / 1024.0
	if kb < 1024 {
		return fmt.Sprintf("%.1fKB", kb)
	}
	mb := kb / 1024.0
	if mb < 1024 {
		return fmt.Sprintf("%.2fMB", mb)
	}
	gb := mb / 1024.0
	return fmt.Sprintf("%.2fGB", gb)
}

// fileMD5 计算指定文件的 MD5 值并以十六进制返回
func fileMD5(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := md5.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
