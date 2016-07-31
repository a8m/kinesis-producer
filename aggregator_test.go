package producer

import (
	"math/rand"
	"strconv"
	"sync"
	"testing"
)

func assert(t *testing.T, val bool, msg string) {
	if !val {
		t.Error(msg)
	}
}

func TestSizeAndCount(t *testing.T) {
	a := new(Aggregator)
	assert(t, a.Size()+a.Count() == 0, "size and count should equal to 0 at the beginning")
	data := []byte("hello")
	pkey := "world"
	n := rand.Intn(100)
	for i := 0; i < n; i++ {
		a.Put(data, pkey)
	}
	assert(t, a.Size() == 5*n+5, "size should equal to the data and the partition-key")
	assert(t, a.Count() == n, "count should be equal to the number of Put calls")
}

func TestAggregation(t *testing.T) {
	var wg sync.WaitGroup
	a := new(Aggregator)
	n := 50
	wg.Add(n)
	for i := 0; i < n; i++ {
		c := strconv.Itoa(i)
		data := []byte("hello-" + c)
		a.Put(data, c)
		wg.Done()
	}
	wg.Wait()
	record, err := a.Drain()
	if err != nil {
		t.Error(err)
	}
	assert(t, isAggregated(record), "should return an agregated record")
	records := extractRecords(record)
	for i := 0; i < n; i++ {
		c := strconv.Itoa(i)
		found := false
		for _, record := range records {
			if string(record.Data) == "hello-"+c {
				assert(t, string(record.Data) == "hello-"+c, "`Data` field contains invalid value")
				found = true
			}
		}
		assert(t, found, "record not found after extracting: "+c)
	}
}
