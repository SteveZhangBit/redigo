package redigo

type PubSub struct{}

func (r *PubSub) NotifyKeyspaceEvent(t int, event string, key []byte, dbid int) {
	if r == nil {
	}
}
