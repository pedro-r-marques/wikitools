package main

import (
	"testing"

	"gotest.tools/assert"
)

func TestDecoder(t *testing.T) {
	data := []struct {
		Input     string
		PageTitle string
		Count     int
	}{
		{"en foo 1 0", "foo", 1},
		{"en foo bar 1 0", "foo bar", 1},
	}
	for _, x := range data {
		title, count, _ := Decode(x.Input)
		assert.Equal(t, title, x.PageTitle)
		assert.Equal(t, count, x.Count)
	}
}
