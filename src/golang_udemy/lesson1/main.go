package main

import (
	"fmt"
)

type UserNotFound struct {
	Username string
}

func (e *UserNotFound) Error() string {
	return fmt.Sprintf("User not found: %v", e.Username)
}

func myFunc() error {
	ok := false
	if ok {
		return nil
	}
	return &UserNotFound{Username: "Mike"}
}

func main() {
	e1 := &UserNotFound{"Mike"}
	e2 := &UserNotFound{"Mike"}
	fmt.Println(e1 == e2)
	if err := myFunc(); err != nil {
		fmt.Println(err)
		if err == e1 {

		}
		if err == e2 {
			
		}
	}
}