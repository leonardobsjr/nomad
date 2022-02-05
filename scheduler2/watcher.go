package scheduler2

import (
	"context"
)

// TODO: Jotting down the inspiration now. Should really go in nomad/nomad
// Not going to refactor Worker right now, but the idea is that this is an
// abstraction over any long running go routine that watches and responds.

type Watcher interface {
	Start()
}

type WatcherConfig struct {
	ctx        context.Context
	cancelFunc context.CancelFunc
}
