package watcher

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
)

// quietPeriod is how long a path has to go quiet before we chunk it.
const quietPeriod = 300 * time.Millisecond

type Watcher struct {
	watcher *fsnotify.Watcher
	rootDir string
	onEvent func(path string)
}

func NewWatcher(rootDir string, onEvent func(string)) (*Watcher, error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	return &Watcher{
		watcher: w,
		rootDir: rootDir,
		onEvent: onEvent,
	}, nil
}

func (w *Watcher) Start(ctx context.Context) error {
	err := filepath.Walk(w.rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			if info.Name()[0] == '.' && path != w.rootDir {
				return filepath.SkipDir
			}

			err = w.watcher.Add(path)
			if err != nil {
				log.Printf(
					"Warning: failed to watch directory %s: %v",
					path,
					err,
				)
			}
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to walk root dir: %w", err)
	}

	go func() {
		var mu sync.Mutex
		timers := make(map[string]*time.Timer)

		fire := func(path string) {
			mu.Lock()
			delete(timers, path)
			mu.Unlock()

			info, err := os.Stat(path)
			if err != nil || info.IsDir() {
				return
			}

			w.onEvent(path)
		}

		for {
			select {
			case <-ctx.Done():
				w.watcher.Close()

				mu.Lock()
				for _, t := range timers {
					t.Stop()
				}
				mu.Unlock()

				return

			case event, ok := <-w.watcher.Events:
				if !ok {
					return
				}

				if event.Op&(fsnotify.Create|fsnotify.Write) == 0 {
					continue
				}

				path := event.Name

				mu.Lock()
				if t, exists := timers[path]; exists {
					t.Reset(quietPeriod)
				} else {
					timers[path] = time.AfterFunc(quietPeriod, func() { fire(path) })
				}
				mu.Unlock()

			case err, ok := <-w.watcher.Errors:
				if !ok {
					return
				}

				log.Printf("Watcher error: %v", err)
			}
		}
	}()

	fmt.Printf("Filesystem watcher active on %s\n", w.rootDir)
	return nil
}
