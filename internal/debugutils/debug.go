package debugutils

import (
	"fmt"

	json "github.com/go-json-experiment/json"
)

func PrettyPrint(qu interface{}) {
	bytes, err := json.Marshal(qu, json.DefaultOptionsV2())
	if err != nil {
		panic(err)
	}

	fmt.Printf("%s", string(bytes))
}

func NotErr(_ any, maybeErr any) {
	if maybeErr != nil {
		panic(maybeErr)
	}
}

func NotNil(maybeNil any) {
	if maybeNil != nil {
		panic(maybeNil)
	}
}
