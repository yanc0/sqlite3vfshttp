package sqlite3vfshttp

import (
	"database/sql"
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/google/go-cmp/cmp"
	_ "github.com/mattn/go-sqlite3"
	"github.com/psanford/sqlite3vfs"
	"github.com/stretchr/testify/assert"
)

type Datasource []byte

func TestSubSlice(t *testing.T) {
	src := Datasource{0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9}
	dest := make([]byte, 4)
	err := offsetCopy[byte](dest, src, 0)
	assert.Nil(t, err)
	err = offsetCopy[byte](dest, src, 7)
	assert.Equal(t, errors.New("read window overflow"), err)
	err = offsetCopy[byte](dest, src, 11)
	assert.Equal(t, errors.New("read window overflow"), err)
	err = offsetCopy[byte](dest, src, 0)
	assert.Nil(t, err)
	assert.Equal(t, []byte{0x0, 0x1, 0x2, 0x3}, dest)
	err = offsetCopy[byte](dest, src, 1)
	assert.Nil(t, err)
	assert.Equal(t, []byte{0x1, 0x2, 0x3, 0x4}, dest)
	err = offsetCopy[byte](dest, src, 6)
	assert.Nil(t, err)
	assert.Equal(t, []byte{0x6, 0x7, 0x8, 0x9}, dest)
}

func TestCache(t *testing.T) {
	const cachesize = 4
	c := cache{off: 0, b: make([]byte, cachesize)}
	src := Datasource{0x0, 0x1, 0x2, 0x3, 0x4, 0x5, 0x6, 0x7, 0x8, 0x9}
	// get a 4 bytes sized window starting from offset 0
	tocache := make([]byte, cachesize)
	err := offsetCopy[byte](tocache, src, 0)
	assert.Nil(t, err)

	// copy data to cache
	copied, err := c.put(tocache, 0)
	assert.Nil(t, err)
	assert.Equal(t, 4, copied)
	assert.Equal(t, int64(0), c.off)
	assert.Equal(t, []byte{0x0, 0x1, 0x2, 0x3}, c.b)

	// get a 4 bytes sized window starting from offset 1
	window := make([]byte, cachesize)
	err = offsetCopy[byte](window, src, 1)
	assert.Nil(t, err)

	// copy window to cache
	copied, err = c.put(window, 1)
	assert.Nil(t, err)
	assert.Equal(t, 4, copied)
	assert.Equal(t, int64(1), c.off)
	assert.Equal(t, []byte{0x1, 0x2, 0x3, 0x4}, c.b)

	window = make([]byte, 2)
	copied, ok := c.get(window, 3)
	assert.Equal(t, true, ok)
	assert.Equal(t, 2, copied)
	assert.Equal(t, []byte{0x3, 0x4}, window)
}

func TestSqlite3vfshttp(t *testing.T) {
	dir, err := ioutil.TempDir("", "sqlite3vfshttptest")
	if err != nil {
		t.Fatal(err)
	}

	db, err := sql.Open("sqlite3", filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS foo (
id text NOT NULL PRIMARY KEY,
title text
)`)
	if err != nil {
		t.Fatal(err)
	}

	rows := []FooRow{
		{
			ID:    "415",
			Title: "romantic-swell",
		},
		{
			ID:    "610",
			Title: "ironically-gnarl",
		},
		{
			ID:    "768",
			Title: "biophysicist-straddled",
		},
	}

	for _, row := range rows {
		_, err = db.Exec(`INSERT INTO foo (id, title) values (?, ?)`, row.ID, row.Title)
		if err != nil {
			t.Fatal(err)
		}
	}

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}

	s := httptest.NewServer(http.FileServer(http.Dir(dir)))

	vfs := HttpVFS{
		URL: s.URL + "/test.db",
	}

	err = sqlite3vfs.RegisterVFS("httpvfs", &vfs)
	if err != nil {
		t.Fatal(err)
	}

	db, err = sql.Open("sqlite3", "meaningless_name.db?vfs=httpvfs&mode=ro")
	if err != nil {
		t.Fatal(err)
	}

	rowIter, err := db.Query(`SELECT id, title from foo order by id`)
	if err != nil {
		t.Fatal(err)
	}

	var gotRows []FooRow

	for rowIter.Next() {
		var row FooRow
		err = rowIter.Scan(&row.ID, &row.Title)
		if err != nil {
			t.Fatal(err)
		}
		gotRows = append(gotRows, row)
	}
	err = rowIter.Close()
	if err != nil {
		t.Fatal(err)
	}

	if !cmp.Equal(rows, gotRows) {
		t.Fatal(cmp.Diff(rows, gotRows))
	}

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}

}

type FooRow struct {
	ID    string
	Title string
}
