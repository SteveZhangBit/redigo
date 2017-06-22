package main

import (
	"flag"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"unsafe"

	"github.com/SteveZhangBit/redigo"
	"github.com/SteveZhangBit/redigo/cmd"
	"github.com/SteveZhangBit/redigo/net"
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

var CommandTable []*redigo.Command = []*redigo.Command{
	{"get", cmd.GETCommand, 2, "rF", 0, 0, 0},
	{"set", cmd.SETCommand, -3, "wm", 0, 0, 0},
	{"setnx", cmd.SETNXCommand, 3, "wmF", 0, 0, 0},
	{"setex", cmd.SETEXCommand, 4, "wm", 0, 0, 0},
	{"psetex", cmd.PSETEXCommand, 4, "wm", 0, 0, 0},
	{"append", cmd.APPENDCommand, 3, "wm", 0, 0, 0},
	{"strlen", cmd.STRLENCommand, 2, "rF", 0, 0, 0},
	{"del", cmd.DELCommand, -2, "w", 0, 0, 0},
	{"exists", cmd.EXISTSCommand, -2, "rF", 0, 0, 0},
	{"setbit", cmd.SETBITCommand, 4, "wm", 0, 0, 0},
	{"getbit", cmd.GETBITCommand, 3, "rF", 0, 0, 0},
	{"setrange", cmd.SETRANGECommand, 4, "wm", 0, 0, 0},
	{"getrange", cmd.GETRANGECommand, 4, "r", 0, 0, 0},
	{"substr", cmd.GETRANGECommand, 4, "r", 0, 0, 0},
	{"incr", cmd.INCRCommand, 2, "wmF", 0, 0, 0},
	{"decr", cmd.DECRCommand, 2, "wmF", 0, 0, 0},
	{"mget", cmd.MGETCommand, -2, "r", 0, 0, 0},
	{"rpush", cmd.RPUSHCommand, -3, "wmF", 0, 0, 0},
	{"lpush", cmd.LPUSHCommand, -3, "wmF", 0, 0, 0},
	{"rpushx", cmd.RPUSHXCommand, 3, "wmF", 0, 0, 0},
	{"lpushx", cmd.LPUSHXCommand, 3, "wmF", 0, 0, 0},
	{"linsert", cmd.LINSERTCommand, 5, "wm", 0, 0, 0},
	{"rpop", cmd.RPOPCommand, 2, "wF", 0, 0, 0},
	{"lpop", cmd.LPOPCommand, 2, "wF", 0, 0, 0},
	{"brpop", cmd.BRPOPCommand, -3, "ws", 0, 0, 0},
	{"brpoplpush", cmd.BRPOPLPUSHCommand, 4, "wms", 0, 0, 0},
	{"blpop", cmd.BLPOPCommand, -3, "ws", 0, 0, 0},
	{"llen", cmd.LLENCommand, 2, "rF", 0, 0, 0},
	{"lindex", cmd.LINDEXCommand, 3, "r", 0, 0, 0},
	{"lset", cmd.LSETCommand, 4, "wm", 0, 0, 0},
	{"lrange", cmd.LRANGECommand, 4, "r", 0, 0, 0},
	{"ltrim", cmd.LTRIMCommand, 4, "w", 0, 0, 0},
	{"lrem", cmd.LREMCommand, 4, "w", 0, 0, 0},
	{"rpoplpush", cmd.RPOPLPUSHCommand, 3, "wm", 0, 0, 0},
	{"sadd", cmd.SADDCommand, -3, "wmF", 0, 0, 0},
	{"srem", cmd.SREMCommand, -3, "wF", 0, 0, 0},
	{"smove", cmd.SMOVECommand, 4, "wF", 0, 0, 0},
	{"sismember", cmd.SISMEMBERCommand, 3, "rF", 0, 0, 0},
	{"scard", cmd.SCARDCommand, 2, "rF", 0, 0, 0},
	{"spop", cmd.SPOPCommand, 2, "wRsF", 0, 0, 0},
	{"srandmember", cmd.SRANDMEMBERCommand, -2, "rR", 0, 0, 0},
	{"sinter", cmd.SINTERCommand, -2, "rS", 0, 0, 0},
	{"sinterstore", cmd.SINTERSTORECommand, -3, "wm", 0, 0, 0},
	{"sunion", cmd.SUNIONCommand, -2, "rS", 0, 0, 0},
	{"sunionstore", cmd.SUNIONSTORECommand, -3, "wm", 0, 0, 0},
	{"sdiff", cmd.SDIFFCommand, -2, "rS", 0, 0, 0},
	{"sdiffstore", cmd.SDIFFSTORECommand, -3, "wm", 0, 0, 0},
	{"smembers", cmd.SINTERCommand, 2, "rS", 0, 0, 0},
	{"sscan", cmd.SSCANCommand, -3, "rR", 0, 0, 0},
	{"zadd", cmd.ZADDCommand, -4, "wmF", 0, 0, 0},
	{"zincrby", cmd.ZINCRBYCommand, 4, "wmF", 0, 0, 0},
	{"zrem", cmd.ZREMCommand, -3, "wF", 0, 0, 0},
	{"zremrangebyscore", cmd.ZREMRANGEBYSCORECommand, 4, "w", 0, 0, 0},
	{"zremrangebyrank", cmd.ZREMRANGEBYRANKCommand, 4, "w", 0, 0, 0},
	{"zremrangebylex", cmd.ZREMRANGEBYLEXCommand, 4, "w", 0, 0, 0},
	{"zunionstore", cmd.ZUNIONSTORECommand, -4, "wm", 0, 0, 0},
	{"zinterstore", cmd.ZINTERSTORECommand, -4, "wm", 0, 0, 0},
	{"zrange", cmd.ZRANGECommand, -4, "r", 0, 0, 0},
	{"zrangebyscore", cmd.ZRANGEBYSCORECommand, -4, "r", 0, 0, 0},
	{"zrevrangebyscore", cmd.ZREVRANGEBYSCORECommand, -4, "r", 0, 0, 0},
	{"zrangebylex", cmd.ZRANGEBYLEXCommand, -4, "r", 0, 0, 0},
	{"zrevrangebylex", cmd.ZREVRANGEBYLEXCommand, -4, "r", 0, 0, 0},
	{"zcount", cmd.ZCOUNTCommand, 4, "rF", 0, 0, 0},
	{"zlexcount", cmd.ZLEXCOUNTCommand, 4, "rF", 0, 0, 0},
	{"zrevrange", cmd.ZREVRANGECommand, -4, "r", 0, 0, 0},
	{"zcard", cmd.ZCARDCommand, 2, "rF", 0, 0, 0},
	{"zscore", cmd.ZSCORECommand, 3, "rF", 0, 0, 0},
	{"zrank", cmd.ZRANKCommand, 3, "rF", 0, 0, 0},
	{"zrevrank", cmd.ZREVRANKCommand, 3, "rF", 0, 0, 0},
	{"zscan", cmd.ZSCANCommand, -3, "rR", 0, 0, 0},
	{"hset", cmd.HSETCommand, 4, "wmF", 0, 0, 0},
	{"hsetnx", cmd.HSETNXCommand, 4, "wmF", 0, 0, 0},
	{"hget", cmd.HGETCommand, 3, "rF", 0, 0, 0},
	{"hmset", cmd.HMSETCommand, -4, "wm", 0, 0, 0},
	{"hmget", cmd.HMGETCommand, -3, "r", 0, 0, 0},
	{"hincrby", cmd.HINCRBYCommand, 4, "wmF", 0, 0, 0},
	{"hincrbyfloat", cmd.HINCRBYFLOATCommand, 4, "wmF", 0, 0, 0},
	{"hdel", cmd.HDELCommand, -3, "wF", 0, 0, 0},
	{"hlen", cmd.HLENCommand, 2, "rF", 0, 0, 0},
	{"hkeys", cmd.HKEYSCommand, 2, "rS", 0, 0, 0},
	{"hvals", cmd.HVALSCommand, 2, "rS", 0, 0, 0},
	{"hgetall", cmd.HGETALLCommand, 2, "r", 0, 0, 0},
	{"hexists", cmd.HEXISTSCommand, 3, "rF", 0, 0, 0},
	{"hscan", cmd.HSCANCommand, -3, "rR", 0, 0, 0},
	{"incrby", cmd.INCRBYCommand, 3, "wmF", 0, 0, 0},
	{"decrby", cmd.DECRBYCommand, 3, "wmF", 0, 0, 0},
	{"incrbyfloat", cmd.INCRBYFLOATCommand, 3, "wmF", 0, 0, 0},
	{"getset", cmd.GETSETCommand, 3, "wm", 0, 0, 0},
	{"mset", cmd.MSETCommand, -3, "wm", 0, 0, 0},
	{"msetnx", cmd.MSETNXCommand, -3, "wm", 0, 0, 0},
	{"randomkey", cmd.RANDOMKEYCommand, 1, "rR", 0, 0, 0},
	{"select", cmd.SELECTCommand, 2, "rlF", 0, 0, 0},
	{"move", cmd.MOVECommand, 3, "wF", 0, 0, 0},
	{"rename", cmd.RENAMECommand, 3, "w", 0, 0, 0},
	{"renamenx", cmd.RENAMENXCommand, 3, "wF", 0, 0, 0},
	{"expire", cmd.EXPIRECommand, 3, "wF", 0, 0, 0},
	{"expireat", cmd.EXPIREATCommand, 3, "wF", 0, 0, 0},
	{"pexpire", cmd.PEXPIRECommand, 3, "wF", 0, 0, 0},
	{"pexpireat", cmd.PEXPIREATCommand, 3, "wF", 0, 0, 0},
	{"keys", cmd.KEYSCommand, 2, "rS", 0, 0, 0},
	{"scan", cmd.SCANCommand, -2, "rR", 0, 0, 0},
	{"dbsize", cmd.DBSIZECommand, 1, "rF", 0, 0, 0},
	{"auth", cmd.AUTHCommand, 2, "rsltF", 0, 0, 0},
	{"ping", cmd.PINGCommand, -1, "rtF", 0, 0, 0},
	{"echo", cmd.ECHOCommand, 2, "rF", 0, 0, 0},
	{"save", cmd.SAVECommand, 1, "ars", 0, 0, 0},
	{"bgsave", cmd.BGSAVECommand, 1, "ar", 0, 0, 0},
	{"bgrewriteaof", cmd.BGREWRITEAOFCommand, 1, "ar", 0, 0, 0},
	{"shutdown", cmd.SHUTDOWNCommand, -1, "arlt", 0, 0, 0},
	{"lastsave", cmd.LASTSAVECommand, 1, "rRF", 0, 0, 0},
	{"type", cmd.TYPECommand, 2, "rF", 0, 0, 0},
	// {"multi", cmd.MULTICommand, 1, "rsF", 0, 0, 0},
	// {"exec", cmd.EXECCommand, 1, "sM", 0, 0, 0},
	// {"discard", cmd.DISCARDCommand, 1, "rsF", 0, 0, 0},
	// {"sync", cmd.SYNCCommand, 1, "ars", 0, 0, 0},
	// {"psync", cmd.SYNCCommand, 3, "ars", 0, 0, 0},
	// {"replconf", cmd.REPLCONFCommand, -1, "arslt", 0, 0, 0},
	{"flushdb", cmd.FLUSHDBCommand, 1, "w", 0, 0, 0},
	{"flushall", cmd.FLUSHALLCommand, 1, "w", 0, 0, 0},
	// {"sort", cmd.SORTCommand, -2, "wm", 0, 0, 0},
	// {"info", cmd.INFOCommand, -1, "rlt", 0, 0, 0},
	// {"monitor", cmd.MONITORCommand, 1, "ars", 0, 0, 0},
	{"ttl", cmd.TTLCommand, 2, "rF", 0, 0, 0},
	{"pttl", cmd.PTTLCommand, 2, "rF", 0, 0, 0},
	{"persist", cmd.PERSISTCommand, 2, "wF", 0, 0, 0},
	// {"slaveof", cmd.SLAVEOFCommand, 3, "ast", 0, 0, 0},
	// {"role", cmd.ROLECommand, 1, "lst", 0, 0, 0},
	// {"debug", cmd.DEBUGCommand, -2, "as", 0, 0, 0},
	{"config", cmd.CONFIGCommand, -2, "art", 0, 0, 0},
	{"subscribe", cmd.SUBSCRIBECommand, -2, "rpslt", 0, 0, 0},
	{"unsubscribe", cmd.UNSUBSCRIBECommand, -1, "rpslt", 0, 0, 0},
	{"psubscribe", cmd.PSUBSCRIBECommand, -2, "rpslt", 0, 0, 0},
	{"punsubscribe", cmd.PUNSUBSCRIBECommand, -1, "rpslt", 0, 0, 0},
	{"publish", cmd.PUBLISHCommand, 3, "pltrF", 0, 0, 0},
	{"pubsub", cmd.PUBSUBCommand, -2, "pltrR", 0, 0, 0},
	// {"watch", cmd.WATCHCommand, -2, "rsF", 0, 0, 0},
	// {"unwatch", cmd.UNWATCHCommand, 1, "rsF", 0, 0, 0},
	// {"cluster", cmd.CLUSTERCommand, -2, "ar", 0, 0, 0},
	// {"restore", cmd.RESTORECommand, -4, "wm", 0, 0, 0},
	// {"restore-asking", cmd.RESTORECommand, -4, "wmk", 0, 0, 0},
	// {"migrate", cmd.MIGRATECommand, -6, "w", 0, 0, 0},
	// {"asking", cmd.ASKINGCommand, 1, "r", 0, 0, 0},
	// {"readonly", cmd.READONLYCommand, 1, "rF", 0, 0, 0},
	// {"readwrite", cmd.READWRITECommand, 1, "rF", 0, 0, 0},
	// {"dump", cmd.DUMPCommand, 2, "r", 0, 0, 0},
	// {"object", cmd.OBJECTCommand, 3, "r", 0, 0, 0},
	{"client", cmd.CLIENTCommand, -2, "rs", 0, 0, 0},
	// {"eval", cmd.EVALCommand, -3, "s", 0, 0, 0},
	// {"evalsha", cmd.EVALSHACommand, -3, "s", 0, 0, 0},
	// {"slowlog", cmd.SLOWLOGCommand, -2, "r", 0, 0, 0},
	// {"script", cmd.SCRIPTCommand, -2, "rs", 0, 0, 0},
	{"time", cmd.TIMECommand, 1, "rRF", 0, 0, 0},
	{"bitop", cmd.BITOPCommand, -4, "wm", 0, 0, 0},
	{"bitcount", cmd.BITCOUNTCommand, -2, "r", 0, 0, 0},
	{"bitpos", cmd.BITPOSCommand, -3, "r", 0, 0, 0},
	// {"wait", cmd.WAITCommand, 3, "rs", 0, 0, 0},
	{"command", cmd.COMMANDCommand, 0, "rlt", 0, 0, 0},
	// {"pfselftest", cmd.PFSELFTESTCommand, 1, "r", 0, 0, 0},
	// {"pfadd", cmd.PFADDCommand, -2, "wmF", 0, 0, 0},
	// {"pfcount", cmd.PFCOUNTCommand, -2, "r", 0, 0, 0},
	// {"pfmerge", cmd.PFMERGECommand, -2, "wm", 0, 0, 0},
	// {"pfdebug", cmd.PFDEBUGCommand, -3, "w", 0, 0, 0},
	// {"latency", cmd.LATENCYCommand, -2, "arslt", 0, 0, 0},
}

var (
	Port = 6379
	BindAddr = []string{""}
)

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

	s := redigo.NewServer(net.NewListener(Port, BindAddr))
	redigo.RedigoLog(redigo.REDIS_NOTICE|redigo.REDIS_LOG_RAW,
		Logo,
		redigo.Version,
		"", "",
		unsafe.Sizeof(int(0))*8,
		"local",
		Port,
		s.PID)
	s.Init(CommandTable)
}
