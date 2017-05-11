package command

import (
	"github.com/SteveZhangBit/redigo"
)

/*-----------------------------------------------------------------------------
 * Type agnostic commands operating on the key space
 *----------------------------------------------------------------------------*/

func FLUSHDBCommand(c redigo.CommandArg) {

}

func FLUSHALLCommand(c redigo.CommandArg) {

}

func DELCommand(c redigo.CommandArg) {
	var deleted int64
	for i := 0; i < c.Argc; i++ {
		c.DB().ExpireIfNeed(c.Argv[i])
		if c.DB().Delete(c.Argv[i]) {
			c.DB().SignalModifyKey(c.Argv[i])
			c.NotifyKeyspaceEvent(redigo.REDIS_NOTIFY_GENERIC, "del", c.Argv[i], c.DB().GetID())
			c.Server().AddDirty(1)
			deleted++
		}
	}
	c.AddReplyInt64(deleted)
}

/* EXISTS key1 key2 ... key_N.
 * Return value is the number of keys existing. */
func EXISTSCommand(c redigo.CommandArg) {

}

func SELECTCommand(c redigo.CommandArg) {

}

func RANDOMKEYCommand(c redigo.CommandArg) {

}

func KEYSCommand(c redigo.CommandArg) {

}

func SCANCommand(c redigo.CommandArg) {

}

func DBSIZECommand(c redigo.CommandArg) {

}

func LASTSAVECommand(c redigo.CommandArg) {

}

func TYPECommand(c redigo.CommandArg) {

}

func RENAMECommand(c redigo.CommandArg) {

}

func RENAMENXCommand(c redigo.CommandArg) {

}

func MOVECommand(c redigo.CommandArg) {

}

/*-----------------------------------------------------------------------------
 * Expire commands
 *----------------------------------------------------------------------------*/

func EXPIRECommand(c redigo.CommandArg) {

}

func EXPIREATCommand(c redigo.CommandArg) {

}

func PEXPIRECommand(c redigo.CommandArg) {

}

func PEXPIREATCommand(c redigo.CommandArg) {

}

func TTLCommand(c redigo.CommandArg) {

}

func PTTLCommand(c redigo.CommandArg) {

}

func PERSISTCommand(c redigo.CommandArg) {

}
