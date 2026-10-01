package testkit

import (
	"reflect"
	"testing"
)

// Equal fails the test if got != want (deep equality).
func Equal[T any](t testing.TB, got, want T, what string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%s:\n got: %#v\nwant: %#v", what, got, want)
	}
}

// True fails the test if cond is false.
func True(t testing.TB, cond bool, format string, args ...any) {
	t.Helper()
	if !cond {
		t.Fatalf(format, args...)
	}
}
