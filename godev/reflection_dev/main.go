package main

import (
	"embed"
	"fmt"
	"reflect"

	cbor "github.com/fxamacker/cbor/v2"
)

//go:embed *
var staticFiles embed.FS

type SomeStruct1 struct {
	A string
	B int
}

type SomeStruct2 struct {
	A string
	B int
	C *string
}

func main() {
	files, _ := staticFiles.ReadDir(".")
	fmt.Printf("files: %v", files)

	someInst := SomeStruct1{
		A: "Qwe",
		B: 10,
	}

	someInstRaw, err := cbor.Marshal(&someInst)
	if err != nil {
		panic(err)
	}

	fmt.Printf("%v\n", someInstRaw)

	stateT := reflect.TypeOf((*SomeStruct1)(nil)).Elem()
	ptr := reflect.New(stateT).Interface()

	fmt.Printf("%+v %+v\n", stateT, ptr)

	err = cbor.Unmarshal(someInstRaw, ptr)
	if err != nil {
		panic(err)
	}

	fmt.Printf("%+v %v\n", ptr, reflect.TypeOf(ptr))

	// stateT2 := reflect.TypeOf(someInst)
	// ptr2 := reflect.New(stateT2).Interface()

	// fmt.Printf("%+v %+v\n", stateT2, ptr2)

	stateT2 := reflect.TypeOf((*SomeStruct2)(nil)).Elem()
	ptr2 := reflect.New(stateT2).Interface()

	err = cbor.Unmarshal(someInstRaw, ptr2)
	if err != nil {
		panic(err)
	}

	fmt.Printf("%+v %v\n", ptr2, reflect.TypeOf(ptr2))
}
