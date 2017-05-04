package redigo

const (
	REDIS_NOTIFY_STRING = iota
	REDIS_NOTIFY_LIST
	REDIS_NOTIFY_HASH
	REDIS_NOTIFY_SET
	REDIS_NOTIFY_ZSET
	REDIS_NOTIFY_GENERIC
)

func NotifyKeyspaceEvent(t int, event string, key string, dbid int) {

}

/*-----------------------------------------------------------------------------
 * Pubsub commands implementation
 *----------------------------------------------------------------------------*/

func SUBSCRIBECommand(c CommandArg) {

}

func UNSUBSCRIBECommand(c CommandArg) {

}

func PSUBSCRIBECommand(c CommandArg) {

}

func PUNSUBSCRIBECommand(c CommandArg) {

}

func PUBLISHCommand(c CommandArg) {

}

func PUBSUBCommand(c CommandArg) {

}
