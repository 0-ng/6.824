package main

import "fmt"

func main() {
	v := []int64{12, 1231, 12312312, 3123}
	a := 5
	b := 3
	if false {
		a = 2
	}
	fmt.Println(v[a:b])
}
