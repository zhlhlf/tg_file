package main

// buildIDs 根据配置重建 IDs 以支持 O(1) 权限查询
func (infos *Infos) buildIDs() {
	infos.Mutex.Lock()
	defer infos.Mutex.Unlock()

	// 检查AdminIDs是否在IDs中
	for _, id := range infos.Conf.AdminIDs {
		if id == 0 {
			continue
		}
		if value, ok := infos.IDs[id]; !ok {
			value.IsAdmin = true
			value.IsWhite = true
			infos.IDs[id] = value
		}
	}

	// 检查WhiteIDs是否在IDs中
	for _, id := range infos.Conf.WhiteIDs {
		if id == 0 {
			continue
		}
		if value, ok := infos.IDs[id]; !ok {
			value.IsWhite = true
			infos.IDs[id] = value
		}
	}
}

func (infos *Infos) isAdmin(id int64) bool {
	if id != 0 && id == infos.currentAdminUserID() {
		return true
	}
	infos.Mutex.RLock()
	defer infos.Mutex.RUnlock()
	if value, ok := infos.IDs[id]; ok {
		return value.IsAdmin
	}
	return false
}

func (infos *Infos) isWhite(id int64) bool {
	infos.Mutex.RLock()
	defer infos.Mutex.RUnlock()
	if value, ok := infos.IDs[id]; ok {
		return value.IsWhite
	}
	return false
}

func (infos *Infos) isInternalUserBot(id int64) bool {
	if id == 0 {
		return false
	}
	infos.Mutex.RLock()
	defer infos.Mutex.RUnlock()
	for _, userID := range infos.UserClientIDs {
		if userID == id {
			return true
		}
	}
	return false
}

func (infos *Infos) firstInternalUserBotID() int64 {
	infos.Mutex.RLock()
	defer infos.Mutex.RUnlock()
	for _, userID := range infos.UserClientIDs {
		if userID != 0 {
			return userID
		}
	}
	return 0
}

func (infos *Infos) notificationTargetID() int64 {
	if infos == nil {
		return 0
	}
	targetID := infos.firstInternalUserBotID()
	if targetID != 0 {
		return targetID
	}
	return 0
}

func (infos *Infos) isAllowedBotSender(id int64) bool {
	return infos.isWhite(id) || infos.isInternalUserBot(id)
}

