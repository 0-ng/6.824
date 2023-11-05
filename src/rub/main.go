package main

import "fmt"

func main() {
	defer func() {
		fmt.Println(1)
		fmt.Println(2)
		fmt.Println(3)
		fmt.Println(4)
	}()
	v := []int64{12, 1231, 12312312, 3123}
	if v[2] != 12331233 {
		v = v[:2]
	}
	fmt.Println(v)
}
