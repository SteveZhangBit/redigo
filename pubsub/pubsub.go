package pubsub

const (
	NotifyString = iota
	NotifyList
	NotifyHash
	NotifySet
	NotifyZSet
	NotifyGeneric
)

func NotifyKeyspaceEvent(t int, event string, key string, dbid int) {

}
