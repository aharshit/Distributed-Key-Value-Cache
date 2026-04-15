package main

import "github.com/aharshit/Distributed-Key-Value-Cache/store"

func main() {
	c := store.NewCluster(5)
	c.Open()
}
