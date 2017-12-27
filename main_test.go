package main

import (
	"testing"
)

func failWhenErr(t *testing.T, err error) {
	if err != nil {
		t.Log(err)
		t.Fail()
	}
}

func failWhen(t *testing.T, x bool) {
	if x {
		t.Fail()
	}
}
