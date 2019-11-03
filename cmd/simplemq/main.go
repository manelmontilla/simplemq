package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/manelmontilla/simplemq"
)

var (
	addr string
)

func main() {
	flag.StringVar(&addr, "addr", "", "addr to listen on, for instace 127.0.0.1:8080")
	flag.Parse()
	if addr == "" {
		fmt.Fprint(os.Stderr, "addr param is required")
		os.Exit(1)
	}
	mq := simplemq.New(addr)
	done := make(chan error)

	go func() {
		err := mq.ListenAndServe()
		done <- err
	}()

	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	var err error
	select {
	case <-c:
		break
	case err = <-done:
		fmt.Fprintf(os.Stderr, "error %+v\n", err)
		break
	}
	if err == nil {
		fmt.Println("stopping simple mq")
	}
	err = mq.Stop()
	if err != nil {
		fmt.Fprint(os.Stderr, "error closing the queue %+v", err)
		os.Exit(1)
	}
}
