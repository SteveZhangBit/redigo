package server

type RedigoPubSub struct{}

func (r *RedigoPubSub) NotifyKeyspaceEvent(t int, event string, key []byte, dbid int) {
	if r == nil {
	}
}
