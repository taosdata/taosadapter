// Copyright (c) 2025 TAOS Data, Inc.
//
// SPDX-License-Identifier: MIT
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package watcher

import (
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/sirupsen/logrus"
)

type fileChangeCallback func(file string, op fsnotify.Op, logger *logrus.Entry)
type Watcher struct {
	filePath string
	watcher  *fsnotify.Watcher
	logger   *logrus.Entry
	cb       fileChangeCallback
	exit     chan struct{}
}

func NewWatcher(logger *logrus.Entry, callback fileChangeCallback, file string) (*Watcher, error) {
	absPath, err := filepath.Abs(filepath.Dir(file))
	if err != nil {
		return nil, err
	}
	fileName := filepath.Base(file)
	filePath := filepath.Join(absPath, fileName)
	logger.Infof("Watching path: %s, file: %s", absPath, filePath)
	w, err := fsnotify.NewWatcher()
	if err != nil {
		logger.Errorf("Error creating watcher: %s", err)
		return nil, err
	}
	err = w.Add(absPath)
	if err != nil {
		logger.Errorf("Error adding path to watcher: %s", err)
		return nil, err
	}
	watcher := &Watcher{
		filePath: filePath,
		watcher:  w,
		logger:   logger,
		cb:       callback,
		exit:     make(chan struct{}),
	}
	go watcher.loop()
	return watcher, nil
}

const DebounceDuration = time.Second

func (w *Watcher) loop() {
	defer func() {
		w.logger.Info("Watcher loop ended")
		close(w.exit)
	}()
	timer := time.NewTimer(DebounceDuration)
	defer func() {
		timer.Stop()
	}()
	timer.Stop()
	lastEvent := fsnotify.Event{}
	for {
		select {
		case event, ok := <-w.watcher.Events:
			if !ok {
				return
			}
			if event.Name == w.filePath && (event.Op.Has(fsnotify.Write) || event.Op.Has(fsnotify.Create)) {
				w.logger.Debugf("Got watch event: %s", event)
				timer.Stop()
				timer.Reset(DebounceDuration)
				lastEvent = event
			}
		case <-timer.C:
			w.logger.Debugf("Debounce period ended, invoking callback for %s", lastEvent)
			w.cb(lastEvent.Name, lastEvent.Op, w.logger)
		case err, ok := <-w.watcher.Errors:
			if !ok {
				return
			}
			w.logger.Errorf("Watcher error: %s", err)
		}
	}
}

func (w *Watcher) Close() error {
	return w.watcher.Close()
}
