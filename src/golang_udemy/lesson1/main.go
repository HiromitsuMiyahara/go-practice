package main

import (
	"fmt"
)

type Human interface {
	Say() string
}

type Pet interface {
	One() string
}

type Person struct {
	Name string
}

func (p *Person) Say() string {
	p.Name = "Mr." + p.Name
	fmt.Println(p.Name)
	return p.Name
}

type Dog struct {
	Name string
}

func (d *Dog) One() string {
	d.Name = "Big" + d.Name
	fmt.Println(d.Name)
	return d.Name
}

func DriveCar(human Human) {
	if human.Say() == "Mr.Mike" {
		fmt.Println("Run")
	} else {
		fmt.Println("Get out")
	}
}

func DriveCar2(pet Pet) {
	if pet.One() == "Bigdog" {
		fmt.Println("Run")
	} else {
		fmt.Println("Get out")
	}
}

func main() {
	var mike Human = &Person{"Mike"}
	var x Human = &Person{"X"}
	var dog Pet = &Dog{"dog"}
	DriveCar(mike)
	DriveCar(x)
	DriveCar2(dog)
}