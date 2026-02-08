package main

import (
	"fmt"
	"os"
)

func foo() {
	defer fmt.Println("world foo")
	fmt.Println("world foo")
}

func main() {
	file, _ := os.Open("./main.go")
	defer file.Close()
	data := make([]byte, 300)
	file.Read(data)
	fmt.Println(string(data))
}