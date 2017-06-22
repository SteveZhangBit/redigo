package net

import (
	"io"
	"net"
	"time"

	"github.com/SteveZhangBit/redigo"
	"github.com/SteveZhangBit/redigo/protocol"
)

const (
	REDIS_SLAVE              = 1 << iota // This client is a slave server
	REDIS_MASTER                         // This client is a master server
	REDIS_MONITOR                        // This client is a slave monitor, see MONITOR
	REDIS_MULTI                          // This client is in a MULTI context
	REDIS_BLOCKED                        // The client is waiting in a blocking operation
	REDIS_DIRTY_CAS                      // Watched keys modified. EXEC will fail.
	REDIS_CLOSE_AFTER_REPLY              // Close after writing entire reply.
	REDIS_UNBLOCKED                      // This client was unblocked and is stored in server.unblocked_clients
	REDIS_LUA_CLIENT                     // This is a non connected client used by Lua
	REDIS_ASKING                         // Client issued the ASKING command
	REDIS_CLOSE_ASAP                     // Close this client ASAP
	REDIS_UNIX_SOCKET                    // Client connected via Unix domain socket
	REDIS_DIRTY_EXEC                     // EXEC will fail for errors while queueing
	REDIS_MASTER_FORCE_REPLY             // Queue replies even if is master
	REDIS_FORCE_AOF                      // Force AOF propagation of current cmd.
	REDIS_FORCE_REPL                     // Force replication of current cmd.
	REDIS_PRE_PSYNC                      // Instance don't understand PSYNC.
	REDIS_READONLY                       // Cluster client is in read-only state.
	REDIS_PUBSUB                         // Client is in Pub/Sub mode.
)

type Connection struct {
	redigo.Reader
	redigo.Writer

	flags   int
	conn    net.Conn
	bstate  blockState
	blocked chan struct{}
}

type blockState struct {
	timeout time.Duration
	keys    map[string]struct{}
}

func NewConn(conn net.Conn) *Connection {
	c := &Connection{conn: conn}
	c.Writer = protocol.NewRESPWriter(c.conn)
	c.Reader = protocol.NewRESPReader(c.conn)
	c.bstate.keys = make(map[string]struct{})
	c.blocked = make(chan struct{})
	return c
}

func (c *Connection) GetAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *Connection) SetBlockTimeout(t time.Duration) {
	c.bstate.timeout = t
}

func (c *Connection) GetBlockedKeys() map[string]struct{} {
	return c.bstate.keys
}

func (c *Connection) Close() error {
	redigo.RedigoLog(redigo.REDIS_DEBUG, "Closing connection on: %s", c.conn.RemoteAddr())
	return c.conn.Close()
}

func (c *Connection) NextCommand(exec redigo.Executor) error {
	if c.flags&REDIS_BLOCKED > 0 {
		c.wait()
	}
	var arg *redigo.CommandArg
	var err error

	if arg, err = c.Read(); err == io.EOF {
		return err
	} else if err != nil {
		c.AddReplyError(err.Error())
		c.setProtocolError()
		return nil
	}
	exec.ProcessCommand(arg)
	if err = c.Flush(); err != nil {
		redigo.RedigoLog(redigo.REDIS_VERBOSE, "Error writing to client: %s", err)
		return err
	} else if c.flags&REDIS_CLOSE_AFTER_REPLY > 0 {
		return io.EOF
	}
	return nil
}

func (c *Connection) setProtocolError() {
	c.flags |= REDIS_CLOSE_AFTER_REPLY
}

func (c *Connection) Block() {
	c.flags |= REDIS_BLOCKED
}

func (c *Connection) Unblock() {
	c.unblock()
	c.blocked <- struct{}{}
}

func (c *Connection) unblock() {
	c.flags &= ^REDIS_BLOCKED
	c.flags |= REDIS_UNBLOCKED
}

func (c *Connection) wait() {
	if c.bstate.timeout > 0 {
		select {
		case <-time.After(c.bstate.timeout):
			c.AddReply(protocol.NullMultiBulk)
			c.unblock()
		case <-c.blocked:
		}
	} else {
		<-c.blocked
	}
}
