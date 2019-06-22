package paxos

import (
  "time"
  "log"
  "fmt"
  "net/http"
)

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
