package main

import (
	"log"
)

const (
	logColorBlue   = "\x1b[34m"
	logColorYellow = "\x1b[33m"
	logColorRed    = "\x1b[31m"
	logColorReset  = "\x1b[0m"
)

func colorLogf(color, format string, args ...any) {
	log.Printf(color+format+logColorReset, args...)
}

func debugf(format string, args ...any) {
	if infos == nil || infos.Conf == nil {
		return
	}
	if infos.Conf.Debug {
		colorLogf(logColorBlue, format, args...)
	}
}

func errorf(format string, args ...any) {
	colorLogf(logColorRed, format, args...)
}

func warnf(format string, args ...any) {
	colorLogf(logColorYellow, format, args...)
}
