package redigo

import (
	"time"
)


type Client struct {
	Connection
	*PubSub

	Server  *Server
	DB      *DB
	lastcmd *Command
}

func NewClient(server *Server, conn Connection) *Client {
	return &Client{Server: server, Connection: conn}
}

func (c *Client) Init() {
	c.SelectDB(0)
	go func() {
		for {
			if err := c.NextCommand(c.Server); err != nil {
				c.Close()
				break
			}
		}
		c.Server.removeClients(c)
	}()
}

func (c *Client) SelectDB(id int) bool {
	if id < 0 || id > len(c.Server.dbs) {
		return false
	} else {
		c.DB = c.Server.dbs[id]
		return true
	}
}

func (c *Client) LookupKeyReadOrReply(key []byte, reply []byte) interface{} {
	if x := c.DB.LookupKeyRead(key); x == nil {
		c.AddReply(reply)
		return nil
	} else {
		return x
	}
}

func (c *Client) LookupKeyWriteOrReply(key []byte, reply []byte) interface{} {
	x := c.DB.LookupKeyWrite(key)
	if x == nil {
		c.AddReply(reply)
	}
	return x
}

// Set a client in blocking mode for the specified key, with the specified timeout
func (c *Client) BlockForKeys(keys [][]byte, timeout time.Duration) {
	c.SetBlockTimeout(timeout)

	for i := 0; i < len(keys); i++ {
		var cls []*Client
		var ok bool

		blockedKeys := c.GetBlockedKeys()
		// If the key already exists in the dict ignore it
		if _, ok = blockedKeys[string(keys[i])]; ok {
			continue
		}
		blockedKeys[string(keys[i])] = struct{}{}
		if cls, ok = c.DB.blockedClients[string(keys[i])]; !ok {
			cls = make([]*Client, 0, 1)
		}
		c.DB.blockedClients[string(keys[i])] = append(cls, c)
	}
	c.Block()
}

/* Block a client for the specific operation type. Once the REDIS_BLOCKED
 * flag is set client query buffer is not longer processed, but accumulated,
 * and will be processed when the client is unblocked. */
func (c *Client) Block() {
	c.Connection.Block()
	c.Server.blockedClients++
}

/* Unblock a client that's waiting in a blocking operation such as BLPOP.
 * You should never call this function directly, but unblockClient() instead. */
func (c *Client) Unblock() {
	for key := range c.GetBlockedKeys() {
		var i int
		var bc *Client

		// Remove this client from the list of clients waiting for this key
		cls := c.DB.blockedClients[key]
		for i, bc = range cls {
			if bc == c {
				break
			}
		}
		c.DB.blockedClients[key] = append(cls[:i], cls[i+1:]...)
		// If the list is empty we need to remove it to avoid wasting memory
		if len(cls) == 0 {
			delete(c.DB.blockedClients, key)
		}

		// Cleanup the client structure
		delete(c.GetBlockedKeys(), key)
	}
	c.Connection.Unblock()
}
