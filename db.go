package filedb

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"errors"
)

var (
	// ErrDBNotFound occurs when a database could not be found.
	ErrDBNotFound = errors.New("database not found; expected existing directory")
)

// Ext is the extension for filedb files.
const Ext string = ".filedb"

var (
	// CNameFormat represents the collection name format.
	CNameFormat = "%s" + Ext
)

// DB represents a database of collections.
type DB struct {
	path string
	cs   map[string]*C
}

// Dial initiates communication with a database.
func Dial(d string) (*DB, error) {
	var err error
	var i os.FileInfo
	if i, err = os.Stat(d); os.IsNotExist(err) {
		return nil, ErrDBNotFound
	}
	if !i.IsDir() {
		return nil, ErrDBNotFound
	}
	return &DB{path: d, cs: make(map[string]*C)}, nil
}

// ColNames gets a list of all collections in the
// database.
func (db *DB) ColNames() ([]string, error) {
	files, err := ioutil.ReadDir(db.path)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, file := range files {
		if strings.ToLower(path.Ext(file.Name())) == Ext {
			names = append(names, file.Name()[0:len(file.Name())-len(Ext)])
		}
	}
	return names, nil
}

// Close closes the database and any open collection
// files.
func (db *DB) Close() {
	for _, c := range db.cs {
		c.close()
	}
}

// C refers to a collection of JSON objects.
func (db *DB) C(name string) (*C, error) {
	if c, ok := db.cs[name]; ok {
		return c, nil
	}
	c := &C{db: db, path: filepath.Join(db.path, fmt.Sprintf(CNameFormat, name))}
	db.cs[name] = c
	return c, nil
}

// C represents a collection of JSON objects.
type C struct {
	db   *DB
	path string
	m    sync.Mutex
	f    *os.File
}

// Path gets the full filepath of the storage for this
// collection.
func (c *C) Path() string {
	return c.path
}

// DB gets the database for this collection.
func (c *C) DB() *DB {
	return c.db
}
func (c *C) close() {
	if c.f != nil {
		c.f.Close()
		c.f = nil
	}
}
func (c *C) file() (*os.File, error) {
	if c.f == nil {
		var err error
		c.f, err = os.OpenFile(c.path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0660)
		if err != nil {
			return nil, err
		}
	}
	return c.f, nil
}

// Drop drops the collection.
func (c *C) Drop() error {
	c.m.Lock()
	defer c.m.Unlock()
	c.close()
	return os.Remove(c.path)
}

// Insert adds a new object to the collection.
func (c *C) Insert(o []byte) error {
	c.m.Lock()
	defer c.m.Unlock()
	f, err := c.file()
	if err != nil {
		return err
	}
	f.Seek(0, os.SEEK_END)
	f.Write(o)
	f.WriteString(fmt.Sprintln())
	return nil
}

// InsertJSON inserts a JSON encoded version of the specified
// object.
func (c *C) InsertJSON(obj interface{}) error {
	b, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	return c.Insert(b)
}

// ForEach iterates over every item in the collection calling
// the function for each row. The function should return true if
// ForEach is to break (stop iterating) at any time.
func (c *C) ForEach(fn func([]byte) bool) error {
	c.m.Lock()
	defer c.m.Unlock()
	f, err := c.file()
	if err != nil {
		return err
	}
	f.Seek(0, os.SEEK_SET)
	s := bufio.NewScanner(f)
	for s.Scan() {
		if fn(s.Bytes()) {
			break
		}
	}
	if s.Err() != nil {
		return s.Err()
	}
	return nil
}
