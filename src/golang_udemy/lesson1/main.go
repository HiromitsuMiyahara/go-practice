package main

import (
	"fmt"
)

type Vertex struct {
	X, Y int
	S    string
}

func chageVertex2(v *Vertex) {
	(*v).X = 1000
}

func main() {
	v := &Vertex{1, 2, "test"}
	chageVertex2(v)
	fmt.Println(v)
}