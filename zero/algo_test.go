package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFindNewAndObsolete(t *testing.T) {

	tests := [][][]uint64{
		{{1, 2, 3}, {2, 3}, {1}, nil},
		{{2, 3}, {4, 5}, {2, 3}, {4, 5}},
		{{2, 3}, {1, 2, 3}, nil, {1}},
	}
	//run each test
	for i := 0; i < len(tests); i++ {
		newids, obselet := findNewAndObsolete(tests[i][0], tests[i][1])
		assert.Equal(t, tests[i][2], newids)
		assert.Equal(t, tests[i][3], obselet)
	}

}
