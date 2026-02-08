package main

import (
	"fmt"
)

type Vertex struct {
	X, Y int
	S    string
}

func chageVertex(v Vertex) {
	v.X = 1000
}

func main() {
	/*
	v2 := Vertex{X: 1}
	fmt.Println(v2)
	fmt.Println(v2.X, v2.Y)
	v2.X = 100
	fmt.Println(v2.X, v2.Y)

	v3 := Vertex{1, 2, "test"}
	fmt.Println(v3)

	v4 := Vertex{}
	fmt.Println(v4)

	var v5 Vertex
	fmt.Println(v5)
	*/

	v6 := new(Vertex)
	fmt.Printf("%T %v\n", v6, v6)

	v7 := &Vertex{}
	fmt.Printf("%T %v\n", v7, v7)
}