package paxos
import (
  "time"
  "log"
  "fmt"
  "sync"
  "math/rand"
  "net/http"
)
const (
  timeout = 10
)


type AcceptRequest struct {
  Sequence int64  `json:"sequence"`
  Master   string `json:"master"`
}

type AcceptResponse struct {
  Status bool `json:"status"`
}

type Server struct {
  lock   *sync.Mutex
  self   string
  seq    int64
  master string
  ns     []string
  http   *http.Client

  promisedSeq int64
}

func NewServer(n string, ns []string) *Server {
  return &Server{
    lock: &sync.Mutex{},
    self: n,
    ns:   ns,
    http: &http.Client{Timeout: time.Second * timeout},
  }
}

func (s *Server) Start() {
  http.HandleFunc("/admin", s.admin)
  http.HandleFunc("/promise", s.promise)
  http.HandleFunc("/accept", s.accept)
  go func() {
    // Figure out how to tell the main thread.
    if err := http.ListenAndServe(s.self, nil); err != nil {
      log.Panicf("could not instantiate the server: %v", err)
    }
  }()

  // Random sleep.
  time.Sleep(time.Duration(rand.Float32() *1000 * timeout) * time.Millisecond)
  go s.startAndWait()

  s.ping()
}

func (s *Server) ping() {
  for ;; {
    time.Sleep(timeout * time.Second)
  }
}


func (s *Server) admin(w http.ResponseWriter, _ *http.Request) {
  if s.isMaster() {
    fmt.Fprintf(w, "Admin page\n\nThis is the master: %s\n", s.self)
  } else {
    fmt.Fprintf(w, "Admin page\n\nThis (%s) isn't the master: %s\n", s.self, s.master)
  }
}

func (s *Server) startAndWait() {
  for ;; {
    if s.isMaster() {
      time.Sleep(timeout * time.Second)
      continue
    }
    // Not the server, check whether it has master.
    if s.isSlave() {
      time.Sleep(timeout * time.Second)
      continue
    }
    // Not the master nor slave.
    electedNode, promisedNodes, err := s.prepare()
    if err != nil {
      time.Sleep(timeout * time.Second)
      continue
    }
    if len(promisedNodes) < len(s.ns) / 2 {
      // Less than half of nodes returned, wait to be pinged and become a slave.
      // TODO(xiaotingye): continue for now.
      time.Sleep(time.Duration(rand.Float32() *1000) * time.Millisecond)
      continue
    }
    // Sleep for a while and check if we become a slave first when waking up.
    time.Sleep(time.Duration(rand.Float32() *5000.0) * time.Millisecond)
    if s.isSlave() {
      continue
    }
    err = s.propose(electedNode, promisedNodes)
    if err != nil {
      log.Printf("failed to propose to master due to %v", err)
      time.Sleep(time.Duration(rand.Float32() *1000) * time.Millisecond)
      continue
    }
    s.updateMasterAndSequence(electedNode.Master, electedNode.Sequence)
  }

}

func (s *Server) isMaster() bool {
  s.lock.Lock()
  defer s.lock.Unlock()
  return s.self == s.master
}

func (s *Server) isSlave() bool {
  s.lock.Lock()
  defer s.lock.Unlock()
  return s.self != s.master && s.master != ""
}

func (s *Server) setPromisedSequence(seq int64) {
  s.lock.Lock()
  defer s.lock.Unlock()
  s.promisedSeq = seq
}

func (s *Server) updateMasterAndSequence(m string, seq int64) {
  s.lock.Lock()
  defer s.lock.Unlock()
  s.promisedSeq = 0
  s.seq = seq
  s.master = m
}

