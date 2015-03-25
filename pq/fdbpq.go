//func (store Store) MergeObject(tr fdb.Transactor, primary string, object map[string]interface{}) (e error) {
//      _, e = tr.Transact(func(tr fdb.Transaction) (interface{}, error) {
//         for k, v := range object {
//             err := store.SetValue(tr, primary, k, v)
//             if err != nil {return nil, err}
//         }
//         return nil, nil
//     })
//     return
// }
// key := p.item.Pack([]tuple.TupleElement{priority})
package pq

import (
	"bytes"
	"errors"
	"github.com/FoundationDB/fdb-go/fdb"
	"github.com/FoundationDB/fdb-go/fdb/directory"
	"github.com/FoundationDB/fdb-go/fdb/subspace"
	"github.com/FoundationDB/fdb-go/fdb/tuple"
	"log"
	"math/rand"
)

func init() {

}

type PriorityQueue struct {
	db       fdb.Database
	subSpace directory.DirectorySubspace

	highContention bool

	// ordered and reversed items
	item, member subspace.Subspace

	// subspaces used for high contention
	popRequest, popResult subspace.Subspace
}

func NewPriorityQueue(db fdb.Database, path []string, hc bool) (PriorityQueue, error) {
	res, err := db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		return directory.CreateOrOpen(tr, path, []byte("fdbpq"))
	})

	if err != nil {
		return PriorityQueue{}, err
	}

	ds, ok := res.(directory.DirectorySubspace)
	if !ok {
		return PriorityQueue{}, errors.New("Could not cast subspace correctly")
	}

	return PriorityQueue{
		db:             db,
		subSpace:       ds,
		highContention: hc,
		item:           ds.Sub("I"),
		member:         ds.Sub("M"),
		popRequest:     ds.Sub("P"),
		popResult:      ds.Sub("R"),
	}, nil
}

// Push adds an item to the queue
func (p *PriorityQueue) Push(priority int, item []byte) error {
	randomID := rand.Int()
	_, err := p.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		if p.checkAtPriority(tr, item, priority) {
			// Push already succeeded
			return nil, nil
		}
		nextKey := p.mustGetNextKey(tr.Snapshot())
		return nil, p.pushAt(tr, item, priority, nextKey, randomID)
	})
	return err
}

func (p *PriorityQueue) Pop() ([]byte, error) {
	return p.pop(false)
}

func (p *PriorityQueue) PopLast() ([]byte, error) {
	return p.pop(true)
}

func (p *PriorityQueue) MustPop() []byte {
	return mustByteWrapper(p.pop(false))
}

func (p *PriorityQueue) MustPopLast() []byte {
	return mustByteWrapper(p.pop(true))
}

// Pop gets the next item from the queue
func (p *PriorityQueue) pop(reverse bool) ([]byte, error) {
	var err error
	var result []byte
	if p.highContention {
		result, err = p.popHighContention(reverse)
	} else {
		result, err = p.popLowContention(reverse)
	}
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (p *PriorityQueue) Peek() ([]byte, error) {
	return p.peek(false)
}

func (p *PriorityQueue) PeekLast() ([]byte, error) {
	return p.peek(true)
}

func (p *PriorityQueue) MustPeek() []byte {
	return mustByteWrapper(p.peek(false))
}

func (p *PriorityQueue) MustPeekLast() []byte {
	return mustByteWrapper(p.peek(true))
}

// Peek gets the next item without popping
func (p *PriorityQueue) peek(reverse bool) ([]byte, error) {
	return interfaceToByteArray(p.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		return p.getFirstItem(tr, reverse)
	}))
}

// Test if queue is empty
func (p *PriorityQueue) IsEmpty() (bool, error) {
	item, err := p.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		return p.getFirstItem(tr, false)
	})
	return item != nil, err
}

func (p *PriorityQueue) MustIsEmpty() bool {
	empty, err := p.IsEmpty()

	if err != nil {
		log.Println(err)
		return true
	}

	if empty {
		return true
	}

	return false
}

// Remove all items from queue
func (p *PriorityQueue) Clear() error {
	_, err := p.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		tr.ClearRange(keyRange(p.subSpace))
		return nil, nil
	})
	return err
}

// Remove item from arbitrary position in the queue
func (p *PriorityQueue) Remove(index int) error {
	_, err := p.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		// TODO
		// for member in tr[self._member[item].range()]:
		//           priority, count = self._member[item].unpack(member.key)
		//           for item_key, value in tr[self._item[priority][count].range()]:
		//               random_id = self._item[priority][count].unpack(item_key)[0]
		//               i = self._decode(value)
		//               if i == item:
		//                   del tr[self._item[priority][count][random_id]]
		//           del tr[self._member[item][priority][count]]
		return nil, nil
	})
	return err
}

func (p *PriorityQueue) checkAtPriority(tr fdb.Transactor, item []byte, priority int) bool {
	// p.member.Unpack(k)
	// TODO
	// r = self._member[item][priority].range()
	//        for _ in tr.get_range(r.start, r.stop, limit=1):
	//            return True
	//        return False
	return false
}

func (p *PriorityQueue) getNextKey(ss fdb.Snapshot) (int, error) {
	lastKey, err := ss.ReadTransact(func(tr fdb.ReadTransaction) (ret interface{}, e error) {
		_, end := p.subSpace.FDBRangeKeys()
		sel := fdb.LastLessThan(end)

		key, err := tr.GetKey(sel).Get()
		if err != nil {
			return nil, err
		}

		keySlice := []byte(key)
		if bytes.Compare(keySlice, []byte(end.FDBKey())) == 1 {
			tpl, err := p.subSpace.Unpack(key)
			if err != nil {
				return nil, err
			}

			if key, ok := tpl[0].(int); ok {
				return key + 1, nil
			}
		}

		return 0, nil
	})

	if err != nil {
		return -1, err
	}

	if lastKey, ok := lastKey.(int); ok {
		return lastKey, nil
	}

	return -1, errors.New("Could not cast key to int")
}

func (p *PriorityQueue) mustGetNextKey(ss fdb.Snapshot) int {
	lastKey, err := p.getNextKey(ss)
	if err != nil {
		log.Println(err.Error())
		return -1
	}
	return lastKey
}

func (p *PriorityQueue) pushAt(tr fdb.Transaction, item []byte, priority, count, randomID int) error {
	key := p.item.Pack([]tuple.TupleElement{priority, count, randomID})
	tr.AddReadConflictKey(key)
	tr.Set(key, item)
	// TODO
	//        tr[self._member[self._decode(item)][priority][count]] = ''
	return nil
}

func (p *PriorityQueue) getFirstItem(tr fdb.Transactor, reverse bool) ([]byte, error) {
	return interfaceToByteArray(tr.ReadTransact(func(rt fdb.ReadTransaction) (ret interface{}, e error) {
		kr := keyRange(p.item)
		resSlice, err := rt.GetRange(kr, fdb.RangeOptions{Limit: 1, Reverse: reverse}).GetSliceWithError()

		if len(resSlice) > 0 {
			return resSlice[1], err
		}

		return nil, err
	}))
}

func (p *PriorityQueue) popLowContention(reverse bool) ([]byte, error) {
	return interfaceToByteArray(p.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		// TODO
		item, err := p.getFirstItem(tr, reverse)

		if item == nil || err != nil {
			return nil, err
		}

		// first_item = self._get_first_item(tr, max)
		//       if first_item is None:
		//           return None
		//       key = first_item.key
		//       item = first_item.value
		//       del tr[key]
		//       priority, count, _ = self._item.unpack(key)
		//       del tr[self._member[self._decode(item)][priority][count]]
		//       return item
		return nil, nil
	}))
}

func (p *PriorityQueue) popHighContention(reverse bool) ([]byte, error) {
	// TODO

	return nil, nil
}

func (p *PriorityQueue) addPopRequest(forced bool) (fdb.Key, error) {
	p.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		// TODO
		// count = self._get_next_count(tr.snapshot, self._pop_request)
		//        if count == 0 and not forced:
		//            return None
		//        request_key = self._pop_request.pack((count, _random_ID()))
		//        # Protect against the unlikely event that someone else got the same
		//        # random ID while adding a pop request.
		//        tr.add_read_conflict_key(request_key)
		//        tr[request_key] = ''
		//        return request_key
		return nil, nil
	})
	// TODO
	return nil, nil
}

// Retrieve and process a batch of requests and a batch of items.
// We initially attempt to retrieve equally sized batches of each. However,
// the number of outstanding requests need not match the number of
// available items; either could be larger than the other. We therefore
// only process a number equal to the smaller of the two.
func (p *PriorityQueue) fulfillRequestedPops(reverse bool) error {
	// TODO

	_, err := p.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		options := fdb.RangeOptions{Limit: 100, Reverse: reverse}
		ss := tr.Snapshot()

		requests := ss.GetRange(keyRange(p.popRequest), options)
		items := ss.GetRange(keyRange(p.item), options)

		reqSlice, err := requests.GetSliceWithError()
		itemSlice, err2 := items.GetSliceWithError()
		if err != nil {
			return nil, err
		}
		if err2 != nil {
			return nil, err2
		}

		for i := 0; i < len(reqSlice); i++ {
			req := reqSlice[i]
			item := itemSlice[i]

			randomID, err := p.popRequest.Unpack(req.Key)
			if err != nil {
				return nil, err
			}

			tr.AddReadConflictKey(req.Key)
			tr.Clear(req.Key)

			if i >= len(itemSlice) {
				// If there are more requests than items, we can delete requests
				// that won't be fulfilled and keep running.
				continue
			}

			subKey := p.popResult.Pack(randomID)
			tr.Set(subKey, item.Value)

			tr.AddReadConflictKey(item.Key)
			tr.Clear(item.Key)

			tpl, err := p.item.Unpack(item.Key)
			if err != nil {
				return nil, err
			}
			priority, count := tpl[0], tpl[1]
			deleteKey := p.member.Pack([]tuple.TupleElement{item.Value, priority, count})
			tr.Clear(deleteKey)
		}
		return
	})
	return err
}

// InterfaceToByteArray is a convenience wrapper used to convert
// the interface{} returned by a transaction to [].
func interfaceToByteArray(i interface{}, err error) ([]byte, error) {
	if err != nil {
		return nil, err
	}

	if ba, ok := i.([]byte); ok {
		return ba, nil
	}

	return nil, errors.New("Couldn't convert to byte array.")
}

// MustByteWrapper is a convenience wrapper to log & continue
// when there are errors.
func mustByteWrapper(i interface{}, err error) []byte {
	ba, err := interfaceToByteArray(i, err)
	if err != nil {
		log.Println(err)
		return nil
	}
	return ba
}

func keyRange(ss subspace.Subspace) fdb.KeyRange {
	begin, end := ss.FDBRangeKeys()
	return fdb.KeyRange{Begin: begin, End: end}
}

// Encode & decode may be useless
// func encode(e tuple.TupleElement) []byte {
// 	t := tuple.Tuple{e}
// 	return t.Pack()
// }

// func decode(e []byte) (tuple.Tuple, error) {
// 	return tuple.Unpack(e)
// }
