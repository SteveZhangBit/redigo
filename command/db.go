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
