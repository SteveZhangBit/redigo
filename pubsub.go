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

func SUBSCRIBECommand(c *RedigoClient) {

}

func UNSUBSCRIBECommand(c *RedigoClient) {

}

func PSUBSCRIBECommand(c *RedigoClient) {

}

func PUNSUBSCRIBECommand(c *RedigoClient) {

}

func PUBLISHCommand(c *RedigoClient) {

}

func PUBSUBCommand(c *RedigoClient) {

}
