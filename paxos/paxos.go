package paxos
import (
  "time"
  "log"
  "fmt"
  "sync"
  "math/rand"
  "net/http"
  "encoding/json"
  "errors"
  "bytes"
)
const (
  timeout = 10
)

type PromiseRequest struct {
  Sequence int64  `json:"sequence"`
}

type PromiseResponse struct {
  Promise  bool   `json:"promise"`
  Sequence int64  `json:"sequence"`
  Master   string `json:"master"`
}

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
  s.promiseAndWait()
}


func (s *Server) admin(w http.ResponseWriter, _ *http.Request) {
  if s.isMaster() {
    fmt.Fprintf(w, "Admin page\n\nThis is the master: %s\n", s.self)
  } else {
    fmt.Fprintf(w, "Admin page\n\nThis (%s) isn't the master: %s\n", s.self, s.master)
  }
}

func (s *Server) promiseAndWait() {
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
    time.Sleep(time.Duration(rand.Float32() *5000.0) * time.Millisecond)
    if s.isSlave() {
      // Sleep for a while and check if we become a slave first when waking up.
      time.Sleep(time.Duration(rand.Float32() *1000) * time.Millisecond)
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

func (s *Server) prepare() (*PromiseResponse, []string, error) {
  seq := time.Now().UTC().Unix()
  promiseReq := &PromiseRequest{Sequence: seq}
  bytes, err := json.Marshal(promiseReq)
  if err != nil {
    return nil, nil, fmt.Errorf("failed to generate prepare request %s due to %v", promiseReq, err)
  }

  var (
    elected *PromiseResponse
    hosts   []string
  )

  for _, n := range s.ns {
    body, err := s.send(bytes, fmt.Sprintf("http://%s/promise", n))
    if err != nil {
      continue
    }
    m, ok := body.(map[string]interface{})
    if !ok {
      log.Printf("failed to convert body %v", body)
      continue
    }
    var pr PromiseResponse
    pr.Promise = m["promise"].(bool)
    pr.Sequence = int64(m["sequence"].(float64))
    pr.Master = m["master"].(string)

    if pr.Promise {
      if elected == nil || elected.Sequence < pr.Sequence {
        elected = &pr
      }
      hosts = append(hosts, n)
    }
  }
  // elected is not nil but not assigned either.
  if elected != nil && elected.Sequence == 0 {
    elected.Sequence = seq
    elected.Master = s.self
  } else if elected == nil {
    elected = &PromiseResponse{Sequence: seq, Master: s.self}
  }
  return elected, hosts, nil
}

func (s *Server) propose(pr *PromiseResponse, hosts []string) error {
  acceptReq := &AcceptRequest{Sequence: pr.Sequence, Master: pr.Master}
  bytes, err := json.Marshal(acceptReq)
  if err != nil {
    return fmt.Errorf("failed to generate prepare request %s due to %v", acceptReq, err)
  }
  count := 0
  // The hosts can be different.
  for _, h := range hosts {
    _, err := s.send(bytes, fmt.Sprintf("http://%s/accept", h))
    if err != nil {
      continue
    }
    count++
  }
  if count < len(s.ns) / 2 {
    err = errors.New("less than half of the nodes failed to ack AcceptRequest")
  }
  return err
}

func (s *Server) promise(w http.ResponseWriter, r *http.Request) {
  r.ParseForm()
  decoder := json.NewDecoder(r.Body)
  defer r.Body.Close()
  var req PromiseRequest
  if err := decoder.Decode(&req); err != nil {
    log.Printf("failed to convert promise request to object: %v", err)
    w.WriteHeader(http.StatusInternalServerError)
    return
  }


  var resp PromiseResponse
  if (s.promisedSeq == 0 || s.promisedSeq < req.Sequence) && req.Sequence > s.seq {
    resp.Promise = true
    if s.seq > 0 {
      // We've received promise request before.
      resp.Sequence = s.seq
      resp.Master = s.master
    }
  }

  w.Header().Set("Content-Type", "application/json")
  w.WriteHeader(http.StatusOK)
  json.NewEncoder(w).Encode(resp)
}

func (s *Server) accept(w http.ResponseWriter, r *http.Request) {
  r.ParseForm()
  decoder := json.NewDecoder(r.Body)
  defer r.Body.Close()
  var req AcceptRequest
  if err := decoder.Decode(&req); err != nil {
    w.WriteHeader(http.StatusInternalServerError)
    return
  }

  w.Header().Set("Content-Type", "application/json")
  w.WriteHeader(http.StatusOK)
  resp := AcceptResponse{Status: true}
  json.NewEncoder(w).Encode(resp)
  s.updateMasterAndSequence(req.Master, req.Sequence)
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

