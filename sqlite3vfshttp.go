package sqlite3vfshttp

import (
	"errors"
	"log"
	"net/http"
	"strings"

	"github.com/psanford/httpreadat"
	"github.com/psanford/sqlite3vfs"
)

type cache struct {
	off int64
	b   []byte
	maxSize int64
}

func (c *cache) size() int64 {
	return int64(2<<21)
}

func (c *cache) put(src []byte, srcOffset int64) (int, error) {
	c.b = src
	c.off = srcOffset
	return len(src), nil
}

func notfound() (int, bool) {
	return 0, false
}

func (c *cache) get(dest []byte, srcOffset int64) (copied int, ok bool) {
	destlen := int64(len(dest))
	realoffset := srcOffset - c.off

	// cache not initialized
	if c.off < 0 {
		return notfound()
	}

	// ask for a non existent offset in cache
	if realoffset < 0 {
		return notfound()
	}
	// window overflow
	if realoffset+destlen > int64(len(c.b)) {
		return notfound()
	}

	err := offsetCopy[byte](dest, c.b, realoffset)
	if err != nil {
		log.Println("get error:", err)
		return notfound()
	}

	return len(dest), true
}

type HttpVFS struct {
	cache          *cache
	URL            string
	TotalReadBytes int64
	CacheHandler   httpreadat.CacheHandler
	RoundTripper   http.RoundTripper
}

func (vfs *HttpVFS) Open(name string, flags sqlite3vfs.OpenFlag) (sqlite3vfs.File, sqlite3vfs.OpenFlag, error) {
	var opts []httpreadat.Option
	if vfs.CacheHandler != nil {
		opts = append(opts, httpreadat.WithCacheHandler(vfs.CacheHandler))
	}
	if vfs.RoundTripper != nil {
		opts = append(opts, httpreadat.WithRoundTripper(vfs.RoundTripper))
	}

	rr := httpreadat.New(vfs.URL, opts...)
	dbSize, err := rr.Size()
	if err != nil {
		return nil, flags, err
	}
	vfs.cache = &cache{
		off: -1,
		b:   make([]byte, 2<<21),
		maxSize: int64(dbSize),
	}
	tf := &httpFile{
		rr:  rr,
		vfs: vfs,
	}

	return tf, flags, nil
}

func (vfs *HttpVFS) Delete(name string, dirSync bool) error {
	return sqlite3vfs.ReadOnlyError
}

func (vfs *HttpVFS) Access(name string, flag sqlite3vfs.AccessFlag) (bool, error) {
	if strings.HasSuffix(name, "-wal") || strings.HasSuffix(name, "-journal") {
		return false, nil
	}

	return true, nil
}

func (vfs *HttpVFS) FullPathname(name string) string {
	return name
}

type httpFile struct {
	rr  *httpreadat.RangeReader
	vfs *HttpVFS
}

func (tf *httpFile) Close() error {
	return nil
}

func (tf *httpFile) ReadAt(p []byte, srcoffset int64) (int, error) {
	// copied, err := tf.rr.ReadAt(p, srcoffset)
	// tf.vfs.TotalReadBytes += int64(copied)
	// log.Printf("network read %d bytes, with offset %d", copied, srcoffset)
	// return copied, err
	copied, ok := tf.vfs.cache.get(p, srcoffset)
	if !ok {
		remote := make([]byte, tf.vfs.cache.size())
		if srcoffset+tf.vfs.cache.size() > tf.vfs.cache.maxSize {
			remote = make([]byte, tf.vfs.cache.maxSize - srcoffset)
		}
		networkReadBytes, err := tf.rr.ReadAt(remote, srcoffset)
		if err != nil {
			return 0, err
		}
		log.Printf("network read %d bytes, with offset %d", networkReadBytes, srcoffset)
		copied, err = tf.vfs.cache.put(remote, srcoffset)
		if err != nil {
			return 0, err
		}
		tf.vfs.TotalReadBytes += int64(copied)
		log.Printf("cache put %d bytes, with offset %d", copied, srcoffset)
		copied, ok = tf.vfs.cache.get(p, srcoffset)
		if !ok {
			return 0, errors.New("cache not found even after put, no bueno")
		}
	}
	log.Printf("cache get %d bytes, with offset %d", copied, srcoffset)
	return copied, nil

}

// offsetCopy fills dest with src starting with offset
// this function can return error if copy overflow
func offsetCopy[T any](dest []T, src []T, offset int64) error {
	if int64(len(dest))+offset > int64(len(src)) {
		return errors.New("read window overflow")
	}
	copy(dest, src[offset:int64(len(dest))+offset])
	return nil
}

func (tf *httpFile) WriteAt(b []byte, off int64) (n int, err error) {
	return 0, sqlite3vfs.ReadOnlyError
}

func (tf *httpFile) Truncate(size int64) error {
	return sqlite3vfs.ReadOnlyError
}

func (tf *httpFile) Sync(flag sqlite3vfs.SyncType) error {
	return nil
}

var invalidContentRangeErr = errors.New("invalid Content-Range response")

func (tf *httpFile) FileSize() (int64, error) {
	return tf.rr.Size()
}

func (tf *httpFile) Lock(elock sqlite3vfs.LockType) error {
	return nil
}

func (tf *httpFile) Unlock(elock sqlite3vfs.LockType) error {
	return nil
}

func (tf *httpFile) CheckReservedLock() (bool, error) {
	return false, nil
}

func (tf *httpFile) SectorSize() int64 {
	return 0
}

func (tf *httpFile) DeviceCharacteristics() sqlite3vfs.DeviceCharacteristic {
	return sqlite3vfs.IocapImmutable
}
