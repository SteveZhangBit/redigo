package shared

const (
	CRLF           = "\r\n"
	OK             = "+OK\r\n"
	Err            = "-ERR\r\n"
	CZero          = ":0\r\n"
	COne           = ":1\r\n"
	CNegOne        = ":-1\r\n"
	NullBulk       = "$-1\r\n"
	EmptyMultiBulk = "*0\r\n"
	WrongTypeErr   = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
	Colon          = ":"
	SyntaxErr      = "-ERR syntax error\r\n"
	NoKeyErr       = "-ERR no such key\r\n"
	OutOfRangeErr  = "-ERR index out of range\r\n"
)
