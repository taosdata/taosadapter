package recordsql

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRecordList(t *testing.T) {
	rl := NewRecordList()
	assert.NotNil(t, rl)
	assert.NotNil(t, rl.list)
	assert.Equal(t, 0, rl.list.Len())
}

func TestRecordList_Add(t *testing.T) {
	t.Run("Single Add", func(t *testing.T) {
		rl := NewRecordList()
		record := &Record{SQL: "SELECT 1"}
		ele := rl.Add(record)

		assert.NotNil(t, ele)
		assert.Equal(t, record, ele.Value.(*Record))
		assert.Equal(t, 1, rl.list.Len())
	})

	t.Run("Concurrent Adds", func(t *testing.T) {
		rl := NewRecordList()
		var wg sync.WaitGroup
		count := 100

		wg.Add(count)
		for i := 0; i < count; i++ {
			go func(i int) {
				defer wg.Done()
				record := &Record{SQL: "SELECT " + string(rune(i))}
				rl.Add(record)
			}(i)
		}
		wg.Wait()

		assert.Equal(t, count, rl.list.Len())
	})
}

func TestRecordList_Remove(t *testing.T) {
	t.Run("Remove Existing Element", func(t *testing.T) {
		rl := NewRecordList()
		record := &Record{SQL: "SELECT 1"}
		ele := rl.Add(record)

		removed := rl.Remove(ele)
		assert.Equal(t, record, removed)
		assert.Equal(t, 0, rl.list.Len())
	})

	t.Run("Remove Nil Element", func(t *testing.T) {
		rl := NewRecordList()
		record := &Record{SQL: "SELECT 1"}
		rl.Add(record)

		removed := rl.Remove(nil)
		assert.Nil(t, removed)
		assert.Equal(t, 1, rl.list.Len())
	})

	t.Run("Remove Already Removed Element", func(t *testing.T) {
		rl := NewRecordList()
		record := &Record{SQL: "SELECT 1"}
		ele := rl.Add(record)
		rl.Remove(ele)

		removed := rl.Remove(ele)
		assert.Nil(t, removed)
		assert.Equal(t, 0, rl.list.Len())
	})

	t.Run("Concurrent Remove", func(t *testing.T) {
		rl := NewRecordList()
		record := &Record{SQL: "SELECT 1"}
		ele := rl.Add(record)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			rl.Remove(ele)
		}()
		go func() {
			defer wg.Done()
			rl.Remove(ele)
		}()
		wg.Wait()

		assert.Equal(t, 0, rl.list.Len())
	})
}

func TestRecordList_RemoveAll(t *testing.T) {
	t.Run("Empty List", func(t *testing.T) {
		rl := NewRecordList()
		records := rl.RemoveAll()
		assert.Empty(t, records)
		assert.Equal(t, 0, rl.list.Len())
	})

	t.Run("Single Item", func(t *testing.T) {
		rl := NewRecordList()
		record := &Record{SQL: "SELECT 1"}
		rl.Add(record)

		records := rl.RemoveAll()
		assert.Equal(t, 1, len(records))
		assert.Equal(t, record, records[0])
		assert.Equal(t, 0, rl.list.Len())
	})

	t.Run("Multiple Items", func(t *testing.T) {
		rl := NewRecordList()
		count := 10
		expectedRecords := make([]*Record, count)

		for i := 0; i < count; i++ {
			record := &Record{SQL: "SELECT " + string(rune(i))}
			expectedRecords[i] = record
			rl.Add(record)
		}

		records := rl.RemoveAll()
		assert.Equal(t, count, len(records))
		assert.ElementsMatch(t, expectedRecords, records)
		assert.Equal(t, 0, rl.list.Len())
	})

	t.Run("Concurrent RemoveAll", func(t *testing.T) {
		rl := NewRecordList()
		count := 100
		for i := 0; i < count; i++ {
			rl.Add(&Record{SQL: "SELECT " + string(rune(i))})
		}

		var wg sync.WaitGroup
		wg.Add(2)
		var records1, records2 []*Record
		go func() {
			defer wg.Done()
			records1 = rl.RemoveAll()
		}()
		go func() {
			defer wg.Done()
			records2 = rl.RemoveAll()
		}()
		wg.Wait()

		// Only one RemoveAll should get all records
		assert.True(t, (len(records1) == count && len(records2) == 0) ||
			(len(records1) == 0 && len(records2) == count))
		assert.Equal(t, 0, rl.list.Len())
	})
}
