package command

import (
	"fmt"
	"os"

	"github.com/SteveZhangBit/redigo"
)

/*============================== client commands ====================================*/
func CLIENTCommand(c redigo.CommandArg) {
}

/*================================= Server Side Commands ===================================== */

func AUTHCommand(c redigo.CommandArg) {

}

func PINGCommand(c redigo.CommandArg) {
	if c.Argc > 2 {
		c.AddReplyError(fmt.Sprintf("wrong number of arguments for '%s' command", c.Argv[0]))
		return
	}

	if c.Argc == 1 {
		c.AddReply(redigo.Pong)
	} else {
		c.AddReplyBulk(c.Argv[1])
	}
}

func ECHOCommand(c redigo.CommandArg) {

}

func TIMECommand(c redigo.CommandArg) {

}

func ADDREPLYCommand(c redigo.CommandArg) {

}

func COMMANDCommand(c redigo.CommandArg) {

}

func SHUTDOWNCommand(c redigo.CommandArg) {
	if c.Argc > 2 {
		c.AddReply(redigo.SyntaxErr)
		return
	}
	/* When SHUTDOWN is called while the server is loading a dataset in
	 * memory we need to make sure no attempt is performed to save
	 * the dataset on shutdown (otherwise it could overwrite the current DB
	 * with half-read data).
	 *
	 * Also when in Sentinel mode clear the SAVE flag and force NOSAVE. */

	if c.Server().PrepareForShutdown() {
		os.Exit(0)
	}
	c.AddReplyError("Errors trying to SHUTDOWN. Check logs.")
}
