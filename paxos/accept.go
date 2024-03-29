package paxos

import (
  "encoding/json"
  "net/http"
  "fmt"
  "errors"
)

func (s *Server) propose(pr *PromiseResponse, hosts []string) error {
  acceptReq := &AcceptRequest{Sequence: pr.Sequence, Master: pr.Master}
  bytes, err := json.Marshal(acceptReq)
  if err != nil {
    return fmt.Errorf("failed to generate accept request %s due to %v", acceptReq, err)
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

