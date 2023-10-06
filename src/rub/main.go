package main

import (
	"fmt"
	"math/rand"
)

func main() {
	lowerLimit := 1
	upperLimit := 10
	fmt.Println(rand.Int()%(upperLimit-lowerLimit) + lowerLimit)
}
