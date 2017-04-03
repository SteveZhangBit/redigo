package pubsub

const (
	NotifyString = iota
	NotifyList
	NotifyHash
	NotifySet
	NotifyGeneric
)

func NotifyKeyspaceEvent(t int, event string, key []byte, dbid int) {

}
