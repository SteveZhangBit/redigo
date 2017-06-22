package net

import (
	"fmt"
	"net"
	"os"

	"github.com/SteveZhangBit/redigo"
)

type Listener struct {
	port        int
	bindAddr    []string
	goListeners []net.Listener
}

func NewListener(port int, addr []string) *Listener {
	return &Listener{port: port, bindAddr: addr}
}

func (l *Listener) Listen() chan redigo.Connection {
	connChan := make(chan redigo.Connection)
	for _, ip := range l.bindAddr {
		addr := fmt.Sprintf("%s;%d", ip, l.port)
		golistener, err := net.Listen("tcp", addr)
		if err != nil {
			redigo.RedigoLog(redigo.REDIS_DEBUG, "Creating Server TCP listening socket %s: %s", addr, err)
			os.Exit(1)
		}

		l.goListeners = append(l.goListeners, golistener)
		go l.accept(golistener, addr, connChan)
	}
	return connChan
}

func (l *Listener) Count() int {
	return len(l.goListeners)
}

func (l *Listener) Close() error {
	for _, golistener := range l.goListeners {
		golistener.Close()
	}
	return nil
}

func (l *Listener) accept(golistener net.Listener, addr string, connChan chan redigo.Connection) {
	defer golistener.Close()
	for {
		var conn net.Conn
		var err error
		if conn, err = golistener.Accept(); err != nil {
			redigo.RedigoLog(redigo.REDIS_DEBUG, "Accepting Server TCP listening socket %s: %s", addr, err)
			break
		}
		connChan <- NewConn(conn)
	}
}
