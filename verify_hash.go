package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/amarnathcjd/gogram/telegram"
)

type fileHashMismatch struct {
	Offset int64
	Limit  int32
}

func fetchTelegramFileHashes(client *telegram.Client, media any, fileSize int64) ([]*telegram.FileHash, error) {
	if client == nil {
		return nil, fmt.Errorf("client 为空")
	}
	location, _, _, _, err := telegram.GetFileLocation(media)
	if err != nil {
		return nil, fmt.Errorf("获取文件定位失败: %w", err)
	}

	seen := make(map[int64]struct{})
	result := make([]*telegram.FileHash, 0, 16)
	offset := int64(0)
	for {
		hashes, err := client.UploadGetFileHashes(location, offset)
		if err != nil {
			return nil, fmt.Errorf("获取 文件分段哈希失败 offset=%d: %w", offset, err)
		}
		if len(hashes) == 0 {
			break
		}

		advanced := false
		for _, h := range hashes {
			if h == nil || h.Limit <= 0 {
				continue
			}
			if _, ok := seen[h.Offset]; ok {
				continue
			}
			seen[h.Offset] = struct{}{}
			result = append(result, h)
			end := h.Offset + int64(h.Limit)
			if end > offset {
				offset = end
				advanced = true
			}
		}
		if !advanced {
			break
		}
		if fileSize > 0 && offset >= fileSize {
			break
		}
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Offset < result[j].Offset
	})
	return result, nil
}

func verifyLocalFileWithTelegramHashes(filePath string, hashes []*telegram.FileHash, fileSize int64) ([]fileHashMismatch, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	mismatches := make([]fileHashMismatch, 0)
	for _, h := range hashes {
		if h == nil || h.Limit <= 0 {
			continue
		}
		if fileSize > 0 && h.Offset >= fileSize {
			continue
		}
		wantLen := int64(h.Limit)
		if fileSize > 0 && h.Offset+wantLen > fileSize {
			wantLen = fileSize - h.Offset
		}
		if wantLen < 0 {
			return nil, fmt.Errorf("无效哈希范围: offset=%d limit=%d", h.Offset, h.Limit)
		}
		buf := make([]byte, wantLen)
		n, err := f.ReadAt(buf, h.Offset)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("读取本地文件失败 offset=%d limit=%d: %w", h.Offset, wantLen, err)
		}
		buf = buf[:n]
		sum := sha256.Sum256(buf)
		if !bytes.Equal(sum[:], h.Hash) {
			mismatches = append(mismatches, fileHashMismatch{Offset: h.Offset, Limit: h.Limit})
		}
	}
	return mismatches, nil
}

func refreshMessageForHashOps(client *telegram.Client, msg telegram.NewMessage) telegram.NewMessage {
	if client == nil || msg.ID == 0 {
		return msg
	}
	ms, err := client.GetMessages(msg.ChatID(), &telegram.SearchOption{IDs: []int32{msg.ID}})
	if err != nil || len(ms) == 0 {
		return msg
	}
	if !ms[0].IsMedia() || ms[0].Media() == nil {
		return msg
	}
	return ms[0]
}

func redownloadMismatchedRanges(client *telegram.Client, media any, filePath string, fileSize int64, mismatches []fileHashMismatch) error {
	if client == nil {
		return fmt.Errorf("client 为空")
	}
	if len(mismatches) == 0 {
		return nil
	}
	f, err := os.OpenFile(filePath, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()

	const chunkSize = 1024 * 1024
	for _, mismatch := range mismatches {
		if mismatch.Limit <= 0 {
			continue
		}
		wantLen := int64(mismatch.Limit)
		if fileSize > 0 && mismatch.Offset+wantLen > fileSize {
			wantLen = fileSize - mismatch.Offset
		}
		if wantLen <= 0 {
			continue
		}
		end := mismatch.Offset + wantLen - 1
		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		content, _, err := client.DownloadChunk(media, int(mismatch.Offset), int(end), chunkSize, false, ctx, 20*time.Second)
		cancel()
		if err != nil {
			return fmt.Errorf("重拉坏块失败 offset=%d limit=%d: %w", mismatch.Offset, mismatch.Limit, err)
		}
		if int64(len(content)) < wantLen {
			return fmt.Errorf("重拉坏块长度不足 offset=%d want=%d actual=%d", mismatch.Offset, wantLen, len(content))
		}
		if int64(len(content)) > wantLen {
			content = content[:wantLen]
		}
		if _, err := f.WriteAt(content, mismatch.Offset); err != nil {
			return fmt.Errorf("覆盖坏块失败 offset=%d limit=%d: %w", mismatch.Offset, mismatch.Limit, err)
		}
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return nil
}

func (infos *Infos) verifyDownloadedFileHashes(sourceClient *telegram.Client, sourceMsg telegram.NewMessage, localPath string) error {
	if infos == nil || sourceClient == nil {
		return nil
	}
	if sourceMsg.Media() == nil || sourceMsg.File == nil || sourceMsg.File.Size <= 0 {
		return nil
	}
	const maxRepairPasses = 2
	refreshedMsg := refreshMessageForHashOps(sourceClient, sourceMsg)

	hashes, err := fetchTelegramFileHashes(sourceClient, refreshedMsg.Media(), refreshedMsg.File.Size)
	if err != nil {
		return err
	}
	if len(hashes) == 0 {
		debugf("未获取到 文件分段哈希，跳过校验: cid=%d mid=%d", refreshedMsg.ChatID(), refreshedMsg.ID)
		return nil
	}

	for pass := 0; pass <= maxRepairPasses; pass++ {
		mismatches, err := verifyLocalFileWithTelegramHashes(localPath, hashes, refreshedMsg.File.Size)
		if err != nil {
			return err
		}
		if len(mismatches) == 0 {
			debugf("文件分段哈希校验通过: cid=%d mid=%d ranges=%d repairPass=%d", refreshedMsg.ChatID(), refreshedMsg.ID, len(hashes), pass)
			return nil
		}

		preview := mismatches
		if len(preview) > 5 {
			preview = preview[:5]
		}
		if pass >= maxRepairPasses {
			return fmt.Errorf("文件分段哈希校验失败: cid=%d mid=%d mismatches=%d sample=%v", refreshedMsg.ChatID(), refreshedMsg.ID, len(mismatches), preview)
		}

		debugf("文件分段哈希校验失败，开始重拉坏块: cid=%d mid=%d pass=%d/%d mismatches=%d sample=%v", refreshedMsg.ChatID(), refreshedMsg.ID, pass+1, maxRepairPasses, len(mismatches), preview)
		refreshedMsg = refreshMessageForHashOps(sourceClient, refreshedMsg)
		if err := redownloadMismatchedRanges(sourceClient, refreshedMsg.Media(), localPath, refreshedMsg.File.Size, mismatches); err != nil {
			return err
		}
	}

	return nil
}
