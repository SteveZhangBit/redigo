package main

import (
	"flag"
	"log"
	"os"
	"runtime/pprof"
	"unsafe"

	"github.com/SteveZhangBit/redigo"
	"github.com/SteveZhangBit/redigo/server"
	"runtime"
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

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile `file`")

func main() {
	// TODO: initServerConfig
	runtime.GOMAXPROCS(1)

	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	s := server.NewServer()
	s.RedigoLog(server.REDIS_NOTICE|server.REDIS_LOG_RAW,
		Logo,
		redigo.Version,
		"", "",
		unsafe.Sizeof(int(0))*8,
		"local",
		s.Port,
		s.PID)
	s.Init()
}
