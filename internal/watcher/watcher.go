package watcher

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
)

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
		debouncer := make(map[string]time.Time)

		for {
			select {
			case <-ctx.Done():
				w.watcher.Close()
				return

			case event, ok := <-w.watcher.Events:
				if !ok {
					return
				}

				if event.Op&(fsnotify.Create|fsnotify.Write) != 0 {
					info, err := os.Stat(event.Name)
					if err != nil || info.IsDir() {
						continue
					}

					if last, ok := debouncer[event.Name]; ok &&
						time.Since(last) < 500*time.Millisecond {
						continue
					}

					debouncer[event.Name] = time.Now()
					w.onEvent(event.Name)
				}

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