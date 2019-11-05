package simplemq

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/julienschmidt/httprouter"
)

type Message struct {
	Received time.Time `json:"Received"`
	Payload  string    `json:"payload"`
}

// SimpleMQ is a very simple queue designed to be used by systems that don't
// high performance queue and/or in memory queues.
type SimpleMQ struct {
	Addr           string
	Queues         map[string][]Message
	Srv            *http.Server
	serverFinished chan error
	sync.Mutex
}

// New creates a new simple queue service that will listen to the given address.
func New(addr string) *SimpleMQ {
	return &SimpleMQ{
		Addr:           addr,
		Queues:         make(map[string][]Message),
		serverFinished: make(chan error, 1),
	}
}

// Enqueue enqueue a message to a queue.
func (s *SimpleMQ) Enqueue(queueID string, m Message) {
	s.Lock()
	defer s.Unlock()
	msgs := s.Queues[queueID]
	msgs = append(msgs, m)
	s.Queues[queueID] = msgs
}

// Dequeue dequeue an element from the given queue.
func (s *SimpleMQ) Dequeue(queueID string) Message {
	s.Lock()
	defer s.Unlock()
	msgs := s.Queues[queueID]
	if len(msgs) < 1 {
		return Message{}
	}
	m := msgs[0]
	s.Queues[queueID] = msgs[1:len(msgs)]
	return m
}

func (s *SimpleMQ) handlePOSTMessage(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {

	id := ps.ByName("queue_id")
	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("error queue_id is mandatory"))
		return
	}

	payload, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusUnprocessableEntity)
		w.Write([]byte(err.Error()))
		return
	}

	if string(payload) == "" {
		w.WriteHeader(http.StatusUnprocessableEntity)
		w.Write([]byte("body can not be empty"))
		return
	}

	// We block the request here to backpresure the consumer if needed.
	m := Message{
		Payload:  string(payload),
		Received: time.Now(),
	}
	s.Enqueue(id, m)
	w.WriteHeader(http.StatusCreated)
}

func (s *SimpleMQ) handleGETMessage(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	id := ps.ByName("queue_id")
	if id == "" {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("error queue_id is mandatory"))
		return
	}

	m := s.Dequeue(id)
	if m.Payload == "" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	e := json.NewEncoder(w)
	err := e.Encode(m)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
}

// ListenAndServe starts de queue, it blocks the calling goroutine.
func (s *SimpleMQ) ListenAndServe() error {
	r := httprouter.New()
	r.POST("/messages/:queue_id", s.handlePOSTMessage)
	r.GET("/messages/:queue_id", s.handleGETMessage)
	server := &http.Server{Addr: s.Addr, Handler: r}
	s.Srv = server
	return server.ListenAndServe()

}

// Stop stops the underlaying http server.
func (s *SimpleMQ) Stop() error {
	ctx := context.Background()
	return s.Srv.Shutdown(ctx)
}
