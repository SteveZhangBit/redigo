package main

import (
	"unsafe"

	"github.com/SteveZhangBit/redigo"
)

const Logo = "\n" +
	"                _._                                                  \n" +
	"           _.-``__ ''-._                                             \n" +
	"      _.-``    `.  `_.  ''-._           Redis %s (%s/%s) %d bit\n" +
	"  .-`` .-```.  ```\\/    _.,_ ''-._                                  \n" +
	" (    '      ,       .-`  | `,    )     Running in %s mode\n" +
	" |`-._`-...-` __...-.``-._|'` _.-'|     Port: %d\n" +
	" |    `-._   `._    /     _.-'    |     PID: %d\n" +
	"  `-._    `-._  `-./  _.-'    _.-'                                   \n" +
	" |`-._`-._    `-.__.-'    _.-'_.-'|                                  \n" +
	" |    `-._`-._        _.-'_.-'    |                                  \n" +
	"  `-._    `-._`-.__.-'_.-'    _.-'                                   \n" +
	" |`-._`-._    `-.__.-'    _.-'_.-'|                                  \n" +
	" |    `-._`-._        _.-'_.-'    |                                  \n" +
	"  `-._    `-._`-.__.-'_.-'    _.-'                                   \n" +
	"      `-._    `-.__.-'    _.-'                                       \n" +
	"          `-._        _.-'                                           \n" +
	"              `-.__.-'                                               \n"

const (
	Version = "0.0.1"
)

func main() {
	// TODO: initServerConfig

	server := redigo.NewServer()
	server.RedigoLog(redigo.REDIS_NOTICE|redigo.REDIS_LOG_RAW, Logo,
		Version,
		"", "",
		unsafe.Sizeof(int(0))*8,
		"local",
		server.Port,
		server.PID)
	server.Init()
}
