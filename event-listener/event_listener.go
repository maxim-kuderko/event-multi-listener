package event_listener

import "github.com/SpotIM/event-multi-listener/types"

type ListenerConfig map[string]interface{}

type EventListener interface {
	Listen(config ListenerConfig ,topicsOutChannels map[string]chan *types.Event,errors chan error)
}
