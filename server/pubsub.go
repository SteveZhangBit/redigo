package server

type RedigoPubSub struct{}

func (r *RedigoPubSub) NotifyKeyspaceEvent(t int, event string, key string, dbid int) {
	if r == nil {
	}
}
