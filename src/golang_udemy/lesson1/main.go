package main

import (
	"fmt"

	"github.com/markcheno/go-quote"
	"github.com/markcheno/go-talib"
)

func foo() {
	defer fmt.Println("world foo")
	fmt.Println("world foo")
}

func main() {
	foo()
	defer fmt.Println("world")
	fmt.Println("hello")
}