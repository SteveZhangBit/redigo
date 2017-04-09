package command

import (
	"github.com/SteveZhangBit/redigo"
)

/*-----------------------------------------------------------------------------
 * Type agnostic commands operating on the key space
 *----------------------------------------------------------------------------*/

func FLUSHDBCommand(c *redigo.RedigoClient) {

}

func FLUSHALLCommand(c *redigo.RedigoClient) {

}

func DELCommand(c *redigo.RedigoClient) {

}

/* EXISTS key1 key2 ... key_N.
 * Return value is the number of keys existing. */
func EXISTSCommand(c *redigo.RedigoClient) {

}

func SELECTCommand(c *redigo.RedigoClient) {

}

func RANDOMKEYCommand(c *redigo.RedigoClient) {

}

func KEYSCommand(c *redigo.RedigoClient) {

}

func SCANCommand(c *redigo.RedigoClient) {

}

func DBSIZECommand(c *redigo.RedigoClient) {

}

func LASTSAVECommand(c *redigo.RedigoClient) {

}

func TYPECommand(c *redigo.RedigoClient) {

}

func SHUTDOWNCommand(c *redigo.RedigoClient) {

}

func RENAMECommand(c *redigo.RedigoClient) {

}

func RENAMEXCommand(c *redigo.RedigoClient) {

}

func MOVECommand(c *redigo.RedigoClient) {

}
