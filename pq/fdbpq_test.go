package pq

import (
	// "fmt"
	"github.com/FoundationDB/fdb-go/fdb"
	// "github.com/FoundationDB/fdb-go/fdb/directory"
	// "github.com/FoundationDB/fdb-go/fdb/subspace"
	// "github.com/FoundationDB/fdb-go/fdb/tuple"
	// "log"
	// "math/rand"
	"testing"
	// "time"
)

var (
	db fdb.Database
	pq PriorityQueue

	testByte1 = []byte("testtest")
	testByte2 = []byte("testTEST")
)

func TestPush(t *testing.T) {
	err := pq.Push(1, testByte)
	if err != nil {
		t.Error("Error pushing to PQ")
	}
}

// func smokeTest(reverse bool) {
// 	log.Println("Running smoke test:")
// 	pq := createQueue(false)

// 	log.Println("Clear Priority Queue")
// 	pq.Clear()
// 	log.Printf("Empty? %s", pq.MustIsEmpty())

// 	log.Println("Push 10, 8, 6")
// 	// pq.Push([]byte(10), 10, randomID())
// 	// pq.Push([]byte(8), 8, randomID())
// 	// pq.Push([]byte(8), 7, randomID())
// 	// pq.Push([]byte(6), 6, randomID())

// 	log.Printf("Empty? %s", pq.MustIsEmpty())
// 	log.Printf("Pop item: %s", pq.MustPop(reverse))
// 	log.Printf("Next item: %s", pq.MustPeek(reverse))
// 	log.Printf("Pop item: %s", pq.MustPop(reverse))
// 	log.Printf("Pop item: %s", pq.MustPop(reverse))
// 	log.Printf("Pop item: %s", pq.MustPop(reverse))
// 	log.Printf("Empty? %s", pq.MustIsEmpty())
// 	log.Println("Push 5")

// 	// pq.Push(5, 5, randomID())
// 	log.Println("Clear Priority Queue")

// 	pq.Clear()
// 	log.Printf("Empty? %s", pq.MustIsEmpty())
// }

// func singleTest(ops int) {
// 	log.Println("Running single client example:")
// 	pq := createQueue(false)
// 	pq.Clear()
// 	for i := 0; i < ops; i++ {
// 		// pq.Push(i, i, randomID())
// 	}

// 	for i := 0; i < ops; i++ {
// 		log.Println(pq.MustPop(false))
// 	}
// }

// func randomID() int {
// 	return rand.Int()
// }

// func producer(pq PriorityQueue, id, total int) {
// 	for i := 0; i < total; i++ {
// 		pq.Push([]byte(fmt.Sprintf("%d.%d", id, i)), id, randomID())
// 	}
// }

// func consumer(pq PriorityQueue, id, total int) {
// 	for i := 0; i < total; i++ {
// 		item := pq.MustPop(false)
// 		if item != nil {
// 			log.Printf("Consumer %d Popped %s", id, item)
// 		} else {
// 			log.Printf("Consumer %d Popped None", id)
// 		}
// 	}
// 	log.Printf("Finished consumer %d", id)
// }

// func createQueue(high bool) PriorityQueue {
// 	pq, err := NewPriorityQueue(db, []string{"fdb", "pq", "test"}, false)
// 	if err != nil {
// 		log.Fatalln(err)
// 	}
// 	return pq
// }

// func multiTest(ops, clients int, high bool) {
// 	description := "low-contention"
// 	if high {
// 		description = "high-contention"
// 	}
// 	log.Println("\nStarting %s test:", description)
// 	pq := createQueue(high)
// 	pq.Clear()
// 	start := time.Now()

// 	for i := 0; i < clients; i++ {
// 		go producer(pq, i, ops)
// 		go consumer(pq, i, ops)
// 	}

// 	log.Printf("Finished %s queue in %v seconds", description, time.Since(start))
// }

func TestMain(m *testing.M) {
	fdb.MustAPIVersion(300)
	db = fdb.MustOpenDefault()
	pq = NewPriorityQueue(db, []string{"testing"}, true)

	// smokeTest(false)
	// smokeTest(true)
	// singleTest(10)
	// multiTest(100, 10, false)
	// multiTest(100, 10, true)
}
