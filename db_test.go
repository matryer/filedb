package filedb_test

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	"github.com/matryer/filedb"
	"github.com/stretchr/testify/require"
)

func setup() {
	os.RemoveAll("test/db")
	os.MkdirAll("test/db", 0777)
}

func TestDial(t *testing.T) {

	db, err := filedb.Dial("test/missing")
	require.Equal(t, err, filedb.ErrDBNotFound)
	require.Nil(t, db)

	db, err = filedb.Dial("test/file.txt")
	require.Equal(t, err, filedb.ErrDBNotFound)
	require.Nil(t, db)

	db, err = filedb.Dial("test/db")
	require.NoError(t, err)
	require.NotNil(t, db)
	db.Close()

}

func TestC(t *testing.T) {

	db, err := filedb.Dial("test/db")
	require.NoError(t, err)
	defer db.Close()
	c, err := db.C("TestCDB")
	require.NoError(t, err)
	require.NotNil(t, c)
	require.Equal(t, c.Path(), "test/db/TestCDB.filedb")
	require.Equal(t, c.DB(), db)

	// same C
	c2, _ := db.C("TestCDB")
	require.Equal(t, c, c2, "Cs with same name should be same object")

}

func TestCollections(t *testing.T) {
	setup()

	db, err := filedb.Dial("test/db")
	require.NoError(t, err)
	defer db.Close()

	c1, _ := db.C("TestCollections1")
	c2, _ := db.C("TestCollections2")
	c3, _ := db.C("TestCollections3")

	c1.Insert([]byte("something"))
	c2.Insert([]byte("something"))
	c3.Insert([]byte("something"))

	cols, err := db.ColNames()
	require.NoError(t, err)

	require.Equal(t, 3, len(cols))
	require.Equal(t, cols[0], "TestCollections1")
	require.Equal(t, cols[1], "TestCollections2")
	require.Equal(t, cols[2], "TestCollections3")

}

func TestInsert(t *testing.T) {

	db, err := filedb.Dial("test/db")
	require.NoError(t, err)
	defer db.Close()
	c, err := db.C("TestInsertDB")

	obj := map[string]interface{}{"name": "Mat", "location": "San Francisco"}
	b, _ := json.Marshal(&obj)
	err = c.Insert(b)
	require.NoError(t, err)

}

func TestSelectEach(t *testing.T) {

	db, err := filedb.Dial("test/db")
	require.NoError(t, err)
	defer db.Close()
	c, err := db.C("TestSelectEachDB")
	c.Drop()

	require.NoError(t, c.InsertJSON(map[string]interface{}{"name": "Mat", "location": "San Francisco"}))
	require.NoError(t, c.InsertJSON(map[string]interface{}{"name": "Ryan", "location": "Costa Rica"}))
	require.NoError(t, c.InsertJSON(map[string]interface{}{"name": "Tyler", "location": "Salt Lake City"}))
	require.NoError(t, c.InsertJSON(map[string]interface{}{"name": "Jeff", "location": "Washington State"}))

	err = c.SelectEach(func(i int, data []byte) (bool, []byte, bool) {
		return i%2 == 0, data, false
	})
	require.NoError(t, err)

	var lines []string
	var sum int
	err = c.ForEach(func(i int, j []byte) bool {
		lines = append(lines, string(j))
		sum = sum + i
		return false // don't break
	})
	require.NoError(t, err)
	require.Equal(t, 2, len(lines))
	require.Equal(t, 1, sum)

}

func TestForEach(t *testing.T) {

	db, err := filedb.Dial("test/db")
	require.NoError(t, err)
	defer db.Close()
	c, err := db.C("TestForEachDB")

	c.Drop()

	require.NoError(t, c.InsertJSON(map[string]interface{}{"name": "Mat", "location": "San Francisco"}))
	require.NoError(t, c.InsertJSON(map[string]interface{}{"name": "Ryan", "location": "Boulder"}))
	require.NoError(t, c.InsertJSON(map[string]interface{}{"name": "Tyler", "location": "Salt Lake City"}))

	var lines []string
	var sum int
	err = c.ForEach(func(i int, j []byte) bool {
		lines = append(lines, string(j))
		sum = sum + i
		return false // don't break
	})
	require.NoError(t, err)
	require.Equal(t, len(lines), 3)
	require.Equal(t, sum, 3)

}

func TestDrop(t *testing.T) {
	db, err := filedb.Dial("test/db")
	require.NoError(t, err)
	defer db.Close()
	c, err := db.C("TestDrop")
	require.NoError(t, c.Drop(), "Drop on no collection should not produce an error")
}

func TestRemoveEach(t *testing.T) {

	db, err := filedb.Dial("test/db")
	require.NoError(t, err)
	defer db.Close()
	c, err := db.C("TestRemoveEachDB")

	require.NoError(t, c.Drop())

	require.NoError(t, c.InsertJSON(map[string]interface{}{"name": "Mat", "location": "San Francisco"}))
	require.NoError(t, c.InsertJSON(map[string]interface{}{"name": "Ryan", "location": "Boulder"}))
	require.NoError(t, c.InsertJSON(map[string]interface{}{"name": "Tyler", "location": "Salt Lake City"}))
	require.NoError(t, c.InsertJSON(map[string]interface{}{"name": "Jeff", "location": "Washington State"}))

	err = c.RemoveEach(func(i int, j []byte) (bool, bool) {
		if bytes.Contains(j, []byte("Ryan")) {
			return true, false // do remove, but don't break
		}
		return false, false // don't remove, don't break
	})
	require.NoError(t, err)

	var lines []string
	var sum int
	err = c.ForEach(func(i int, j []byte) bool {
		lines = append(lines, string(j))
		sum = sum + i
		return false // don't break
	})
	require.NoError(t, err)
	require.Equal(t, 3, len(lines), "should be two lines after one was removed")
	require.Equal(t, 3, sum)

}
