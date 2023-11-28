package sqlite3vfshttp_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type Datasource []byte

func TestBytesCache(t *testing.T) {
	orig := Datasource{0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9}

	toFill := make([]byte, 4)

	assert.Equal(t, 10, len(orig))

	read, err := orig.ReadAt(toFill, 0)
	assert.Nil(t, err)
	assert.Equal(t, int64(4), read)
	assert.Equal(t, []byte{0x0, 0x1, 0x2, 0x3}, toFill)
}

func (d Datasource) ReadAt(p []byte, off int64) (int64, error) {

	copy(p, d[off:len(p)])
	return int64(len(p)), nil
}
