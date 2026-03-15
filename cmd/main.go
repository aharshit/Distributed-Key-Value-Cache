package main

import "github.com/user/kvcache/store"

func main() {
	c := store.NewCluster(5)
	c.Open()
}
