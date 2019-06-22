package paxos
import (
  "time"
  "log"
  "fmt"
  "sync"
  "math/rand"
  "net/http"
  "bytes"
  "encoding/json"
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

  pChan       chan bool
  mChan       chan bool
  duration    time.Duration
  promisedSeq int64
}

func NewServer(n string, ns []string) *Server {
  return &Server{
    lock: &sync.Mutex{},
    self: n,
    ns:   ns,
    // If no heartbeat for more than 100 seconds, re-elect the master.
    duration: timeout * 10 * time.Second,
    http:     &http.Client{Timeout: time.Second * timeout},
    pChan:    make(chan bool),
    mChan:    make(chan bool),
  }
}

func (s *Server) Start() {
  http.HandleFunc("/admin", s.admin)
  http.HandleFunc("/promise", s.promise)
  http.HandleFunc("/accept", s.accept)
  http.HandleFunc("/ping", s.ping)
  go func() {
    // Figure out how to tell the main thread.
    if err := http.ListenAndServe(s.self, nil); err != nil {
      log.Panicf("could not instantiate the server: %v", err)
    }
  }()

  // Random sleep.
  time.Sleep(time.Duration(rand.Float32() *1000 * timeout) * time.Millisecond)
  go s.startAndWait()

  log.Printf("%s starts to monitor\n", s.self)
  s.monitor()
  defer close(s.pChan)
  defer close(s.mChan)
}

func (s *Server) monitor() {
  t := time.NewTimer(s.duration)
  defer t.Stop()
  for ;; {
    if s.isMaster() {
      count := 0
      for _, n := range s.ns {
        // Ping each slave. If majority of them still respond, the master is
        // maintained. Otherwise clear the master and re-elect.
        resp, err := s.http.Get(fmt.Sprintf("http://%s/ping", n))
        if err != nil || resp.StatusCode != 200 {
          log.Printf("failed to ping %s\n", n)
        } else {
          log.Printf("ping %s successfully\n", n)
          count++
        }
      }
      if count < len(s.ns) / 2 {
        s.updateMasterAndSequence("", 0)
      } else {
        time.Sleep(timeout * time.Second)
        t.Reset(s.duration)
      }
      continue
    }
    select {
    case <-t.C:
      s.updateMasterAndSequence("", 0)
    case <-s.pChan:
      t.Reset(s.duration)
    case <-s.mChan:
      t.Reset(s.duration)
    }
  }
}

func (s *Server) ping(w http.ResponseWriter, _ *http.Request) {
  w.WriteHeader(http.StatusOK)
  s.pChan <- true
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
    s.mChan <- true
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

func (s *Server) send(data []byte, url string) (interface{}, error) {
  response, err := s.http.Post(url, "application/json", bytes.NewBuffer(data))
  if err != nil {
    log.Printf("failed to send request to %s due to %v", url, err)
    return nil, err
  }
  if response.StatusCode != 200 {
    log.Printf("non 200 HTTP status returned by %s, code: %d", url, response.StatusCode)
    return nil, err
  }

  var body interface{}
  decoder := json.NewDecoder(response.Body)
  if err = decoder.Decode(&body); err != nil {
    log.Printf("failed to parse response %s due to %v", response, err)
  }
  return body, err
}

