package redigo

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/SteveZhangBit/redigo/rtype"
	"github.com/SteveZhangBit/redigo/util"
)

type Server struct {
	PID             int
	Commands        map[string]*Command
	DBNums          int
	Dirty           int
	KeyspaceMisses  int
	KeyspaceHits    int
	StatStartTime   time.Time
	StatNumCommands int

	clients        []*Client
	dbs            []*DB
	rwlock         sync.RWMutex
	listener       Listener
	blockedClients int
	readyKeys      []readyKey
}

/* The following structure represents a node in the server.ready_keys list,
 * where we accumulate all the keys that had clients blocked with a blocking
 * operation such as B[LR]POP, but received new data in the context of the
 * last executed command.
 *
 * After the execution of every command or script, we run this list to check
 * if as a result we should serve data to clients blocked, unblocking them.
 * Note that server.ready_keys will not have duplicates as there dictionary
 * also called ready_keys in every structure representing a Redis database,
 * where we make sure to remember if a given key was already added in the
 * server.ready_keys list. */
type readyKey struct {
	db  *DB
	key []byte
}

func NewServer(listener Listener) *Server {
	s := &Server{
		PID:           os.Getpid(),
		StatStartTime: time.Now(),
		DBNums:        4,
		listener:      listener,
	}
	// Create the Redis databases, and initialize other internal state.
	s.dbs = make([]*DB, s.DBNums)
	for i := 0; i < s.DBNums; i++ {
		s.dbs[i] = NewDB(s, i)
	}
	return s
}

func (s *Server) Init(commandTable []*Command) {
	s.populateCommandTable(commandTable)

	// Open TCP listening socket for the user commands.
	connChan := s.listener.Listen()
	if s.listener.Count() == 0 {
		RedigoLog(REDIS_WARNING, "Configured to not listen anywhere, exiting.")
		os.Exit(1)
	}
	// Add system interrupt listener
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	for {
		select {
		case conn := <-connChan:
			s.addClients(conn)
		case <-interrupt:
			RedigoLog(REDIS_WARNING, "Received SIGINT scheduling shutdown...")
			if s.PrepareForShutdown() {
				return
			}
			RedigoLog(REDIS_WARNING, "SIGTERM received but errors trying to shutdown the server, check the logs for more information.")
		}
	}
}

// Populates the Redis Command Table starting from the hard coded list
func (s *Server) populateCommandTable(commandTable []*Command) {
	s.Commands = make(map[string]*Command)
	for _, cmd := range commandTable {
		s.Commands[cmd.Name] = cmd

		for _, c := range cmd.SFlags {
			switch c {
			case 'w':
				cmd.Flags |= REDIS_CMD_WRITE
			case 'r':
				cmd.Flags |= REDIS_CMD_READONLY
			case 'm':
				cmd.Flags |= REDIS_CMD_DENYOOM
			case 'a':
				cmd.Flags |= REDIS_CMD_ADMIN
			case 'p':
				cmd.Flags |= REDIS_CMD_PUBSUB
			case 's':
				cmd.Flags |= REDIS_CMD_NOSCRIPT
			case 'R':
				cmd.Flags |= REDIS_CMD_RANDOM
			case 'S':
				cmd.Flags |= REDIS_CMD_SORT_FOR_SCRIPT
			case 'l':
				cmd.Flags |= REDIS_CMD_LOADING
			case 't':
				cmd.Flags |= REDIS_CMD_STALE
			case 'M':
				cmd.Flags |= REDIS_CMD_SKIP_MONITOR
			case 'k':
				cmd.Flags |= REDIS_CMD_ASKING
			case 'F':
				cmd.Flags |= REDIS_CMD_FAST
			default:
				panic("Unsupported command flag")
			}
		}
	}
}

func (s *Server) addClients(conn Connection) {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()
	c := NewClient(s, conn)
	c.Init()
	s.clients = append(s.clients, c)
	RedigoLog(REDIS_DEBUG, "Add client on %v", c.GetAddr())
}

func (s *Server) removeClients(c *Client) {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()

	var i int
	for i = 0; i < len(s.clients); i++ {
		if c == s.clients[i] {
			break
		}
	}
	s.clients = append(s.clients[:i], s.clients[:i+1]...)
	RedigoLog(REDIS_DEBUG, "Remove client on %v", c.GetAddr())
}

/* If this function gets called we already read a whole
 * command, arguments are in the client argv/argc fields.
 * processCommand() execute the command or prepare the
 * server for a bulk read from the client.
 *
 * If 1 is returned the client is still alive and valid and
 * other operations can be performed by the caller. Otherwise
 * if 0 is returned the client was destroyed (i.e. after QUIT). */
func (s *Server) ProcessCommand(arg *CommandArg) {
	/* Now lookup the command and check ASAP about trivial error conditions
	 * such as wrong arity, bad command name and so forth. */

	cmd, ok := s.Commands[string(util.ToLower(arg.Argv[0]))]
	arg.Client.lastcmd = cmd
	if !ok {
		arg.AddReplyError(fmt.Sprintf("unknown command '%s'", string(arg.Argv[0])))
		return
	} else if (cmd.Arity > 0 && cmd.Arity != arg.Argc) || (arg.Argc < -cmd.Arity) {
		arg.AddReplyError(fmt.Sprintf("wrong number of arguments for '%s' command", cmd.Name))
		return
	}

	s.call(arg, cmd)
	return
}

func (s *Server) call(arg *CommandArg, cmd *Command) {
	// Lock
	if cmd.Flags&REDIS_CMD_WRITE > 0 {
		s.rwlock.Lock()
	} else {
		s.rwlock.RLock()
	}

	/* Call the command. */
	dirty := s.Dirty
	start := time.Now()
	cmd.Proc(arg)
	duration := time.Now().Sub(start)
	dirty = s.Dirty - dirty
	if dirty < 0 {
		dirty = 0
	}

	cmd.MicroSeconds += int64(duration / time.Microsecond)
	cmd.Calls++

	s.StatNumCommands++
	// If there are clients blocked on lists
	if len(s.readyKeys) > 0 {
		s.handleClientsBlockedOnLists()
	}

	// Unlock and return
	if cmd.Flags&REDIS_CMD_WRITE > 0 {
		s.rwlock.Unlock()
	} else {
		s.rwlock.RUnlock()
	}
}

/* This function should be called by Redis every time a single command,
 * a MULTI/EXEC block, or a Lua script, terminated its execution after
 * being called by a client.
 *
 * All the keys with at least one client blocked that received at least
 * one new element via some PUSH operation are accumulated into
 * the server.ready_keys list. This function will run the list and will
 * serve clients accordingly. Note that the function will iterate again and
 * again as a result of serving BRPOPLPUSH we can have new blocking clients
 * to serve because of the PUSH side of BRPOPLPUSH. */
func (s *Server) handleClientsBlockedOnLists() {
	l := s.readyKeys
	for len(l) > 0 {
		rk := l[0]
		/* First of all remove this key from db->ready_keys so that
		 * we can safely call signalListAsReady() against this key. */
		delete(rk.db.readyKeys, string(rk.key))

		/* If the key exists and it's a list, serve blocked clients
		 * with data. */
		if o, ok := rk.db.LookupKeyWrite(rk.key).(rtype.List); ok {
			/* We serve clients in the same order they blocked for
			 * this key, from the first blocked to the last. */
			if cls, ok := rk.db.blockedClients[string(rk.key)]; ok {
				for i := 0; i < len(cls); i++ {
					var where int
					var receiver *Client = cls[i]
					var val rtype.String

					if receiver.lastcmd != nil && receiver.lastcmd.Name == "blpop" {
						where = rtype.REDIS_LIST_HEAD
						val = o.PopFront().Value()
					} else {
						where = rtype.REDIS_LIST_TAIL
						val = o.PopBack().Value()
					}

					if val != nil {
						receiver.Unblock()
						if !s.serveClientBlockedOnList(receiver, rk.key, rk.db, val, where) {
							/* If we failed serving the client we need
							 * to also undo the POP operation. */
							if where == rtype.REDIS_LIST_HEAD {
								o.PushFront(val)
							} else {
								o.PushBack(val)
							}
						}
					} else {
						// ???
						break
					}
				}
			}
			if o.Len() == 0 {
				rk.db.Delete(rk.key)
			}
			/* We don't call signalModifiedKey() as it was already called
			 * when an element was pushed on the list. */
		}
		l = append(l[:0], l[1:]...)
	}
}

/* This is a helper function for handleClientsBlockedOnLists(). It's work
 * is to serve a specific client (receiver) that is blocked on 'key'
 * in the context of the specified 'db', doing the following:
 *
 * 1) Provide the client with the 'value' element.
 * 2) If the dstkey is not NULL (we are serving a BRPOPLPUSH) also push the
 *    'value' element on the destination list (the LPUSH side of the command).
 * 3) Propagate the resulting BRPOP, BLPOP and additional LPUSH if any into
 *    the AOF and replication channel.
 *
 * The argument 'where' is REDIS_TAIL or REDIS_HEAD, and indicates if the
 * 'value' element was popped fron the head (BLPOP) or tail (BRPOP) so that
 * we can propagate the command properly.
 *
 * The function returns REDIS_OK if we are able to serve the client, otherwise
 * REDIS_ERR is returned to signal the caller that the list POP operation
 * should be undone as the client was not served: This only happens for
 * BRPOPLPUSH that fails to push the value to the destination key as it is
 * of the wrong type. */
func (s *Server) serveClientBlockedOnList(receiver *Client, key []byte, db *DB, val rtype.String, where int) bool {
	receiver.AddReplyMultiBulkLen(2)
	receiver.AddReplyBulk(key)
	receiver.AddReplyBulk(val.Bytes())
	return true
}

/*=========================================== Shutdown ======================================== */

func (s *Server) PrepareForShutdown() bool {
	RedigoLog(REDIS_WARNING, "User requested shutdown...")
	s.listener.Close()
	RedigoLog(REDIS_WARNING, "%s is now ready to exit, bye bye...", "Redis")
	return true
}
