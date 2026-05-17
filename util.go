package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

// handleTime 将秒数格式化为人类可读的时间字符串
func handleTime(secs uint64) string {
	if secs > 86400 {
		return fmt.Sprintf("%dd %dh %dm %ds", secs/86400, (secs%86400)/3600, (secs%3600)/60, secs%60)
	} else if secs > 3600 {
		return fmt.Sprintf("%dh %dm %ds", secs/3600, (secs%3600)/60, secs%60)
	} else if secs > 60 {
		return fmt.Sprintf("%dm %ds", secs/60, secs%60)
	}
	return fmt.Sprintf("%ds", secs)
}

// extractContent 从字符串中提取正文与可选的行数参数
// 例如 "error 20" 返回 ("error", &20)；"error" 返回 ("error", nil)；"20" 返回 ("", &20)
func extractContent(src string) (string, *int) {
	src = strings.TrimSpace(src)

	// 1. 如果整个字符串就是一个数字
	if num, err := strconv.Atoi(src); err == nil {
		return "", &num
	}

	// 2. 寻找主体部分最后一个空格
	count := strings.LastIndexByte(src, ' ')
	if count == -1 {
		return src, nil
	}

	// 3. 判断最后一个空格后面那一截是不是数字
	content := src[count+1:]
	if num, err := strconv.Atoi(content); err == nil {
		return src[:count], &num
	}

	return src, nil
}

// readLastLines 读取日志文件中匹配 src 正则的最后 count 行
func readLastLines(filePath, src string, count int) (lines []string, err error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Printf("关闭文件失败: %+v", err)
		}
	}()

	re := regexp.MustCompile(src)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if re.MatchString(scanner.Text()) {
			lines = append(lines, scanner.Text())
		}
		// 超过行数限制后, 舍弃旧行（滑动窗口效果）
		if len(lines) > count {
			lines = lines[1:]
		}
	}
	if err := scanner.Err(); err != nil {
		return lines, err
	}
	return lines, nil
}

// cleanTmpDir 删除并重建临时下载目录，确保每次运行都从干净状态开始
func cleanTmpDir() error {
	if infos == nil || infos.FilesPath == "" {
		return nil
	}
	tmpDir := filepath.Join(infos.FilesPath, "tmp")
	if err := os.RemoveAll(tmpDir); err != nil {
		return err
	}
	return os.MkdirAll(tmpDir, 0755)
}

// isDigit 判断 rune 是否为数字字符（供 submitCode 过滤验证码使用）
func isDigit(r rune) bool {
	return r >= '0' && r <= '9'
}
