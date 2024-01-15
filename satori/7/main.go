package main

import (
	"fmt"
	"reflect"
	"runtime"
)

type Dummy struct{}

func (dummy Dummy) Hello() {
	fmt.Println("hello")
}

func GetFunctionName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}

func main() {
	d := Dummy{}
	fmt.Println(GetFunctionName(d.Hello))
}
