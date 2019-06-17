package main

import (
  "flag"
  "./paxos"
  "strings"
)


func main() {
	var host = flag.String("host", "", "The host of this server")
	var allHosts = flag.String("all-hosts", "", "The hosts of all servers")
	hosts := strings.Split(*allHosts, ";")

  var nodes []string
	for _, h := range hosts {
		if *host != h {
			nodes = append(nodes, h)
		}
	}
	s := paxos.NewServer(*host, nodes)
	s.Start()
}