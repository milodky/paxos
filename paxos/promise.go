package paxos

import (
  "time"
  "log"
  "fmt"
  "net/http"
  "encoding/json"
)

type PromiseRequest struct {
  Sequence int64  `json:"sequence"`
}

type PromiseResponse struct {
  Promise  bool   `json:"promise"`
  Sequence int64  `json:"sequence"`
  Master   string `json:"master"`
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
