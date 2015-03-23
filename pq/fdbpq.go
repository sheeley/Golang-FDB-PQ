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

package pq

import (
	"errors"
	"github.com/FoundationDB/fdb-go/fdb"
	"github.com/FoundationDB/fdb-go/fdb/directory"
	"github.com/FoundationDB/fdb-go/fdb/subspace"
	"github.com/FoundationDB/fdb-go/fdb/tuple"
	"log"
)

func init() {
	fdb.MustAPIVersion(300)
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
func (p *PriorityQueue) Push(item []byte, priority, randomID int) error {
	_, err := p.db.Transact(func(tr fdb.Transaction) (ret interface{}, e error) {
		if p.checkAtPriority(tr, item, priority) {
			return nil, nil
		}
		key := p.item.Pack([]tuple.TupleElement{priority})
		nextKey := p.getNextKey(tr.Snapshot(), key)
		return nil, p.pushAt(tr, item, nextKey, priority, randomID)
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
		first, err := p.getFirstItem(tr, reverse)
		if err != nil {
			return nil, err
		} else if first != nil {
			return first, nil
		}
		return nil, nil
	}))
}

//tr fdb.Transactor

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
		begin, end := p.subSpace.FDBRangeKeys()
		tr.ClearRange(fdb.KeyRange{Begin: begin, End: end})
		return nil, nil
	})
	return err
}

// Remove item from arbitrary index in the queue
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

func (p *PriorityQueue) checkAtPriority(tr fdb.Transaction, item []byte, priority int) bool {
	p.member.Unpack(k)
	// TODO
	// r = self._member[item][priority].range()
	//        for _ in tr.get_range(r.start, r.stop, limit=1):
	//            return True
	//        return False
	return false
}

func (p *PriorityQueue) getNextKey(ss fdb.Snapshot, key fdb.Key) int {
	ss.ReadTransact(func(rt fdb.ReadTransaction) (ret interface{}, e error) {
		start, end := p.subSpace.FDBRangeKeys()
		sel := fdb.LastLessThan(end)
		lastKey := tr.GetKey(sel)
		return lastKey, nil

	})

	if lastKey >= end {
		return p.subSpace.Unpack(lastKey) + 1
	}

	return 0
}

func (p *PriorityQueue) pushAt(tr fdb.Transaction, item []byte, count, priority, randomID int) error {
	var key fdb.Key
	tr.AddReadConflictKey(key)
	// TODO
	// key = self._item[priority][count][random_ID]
	//        # Protect against the unlikely event that another transaction pushing a
	//        # an item with the same priority got the same count and random ID.
	//        tr.add_read_conflict_key(key)
	//        tr[key] = item
	//        tr[self._member[self._decode(item)][priority][count]] = ''
	return nil
}

func (p *PriorityQueue) getFirstItem(tr fdb.Transaction, reverse bool) ([]byte, error) {
	// TODO
	//r = self._item.range()
	//       for kv in tr.get_range(r.start, r.stop, limit=1, reverse=max):
	//           return kv
	//       return None
	return nil, nil
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

func (p *PriorityQueue) fulfillRequestedPops(reverse bool) error {
	// TODO
	// ''' Retrieve and process a batch of requests and a batch of items.

	//        We initially attempt to retrieve equally sized batches of each. However,
	//        the number of outstanding requests need not match the number of
	//        available items; either could be larger than the other. We therefore
	//        only process a number equal to the smaller of the two.
	//        '''
	//        batch = 100

	//        tr = db.create_transaction()
	//        r = self._pop_request.range()
	//        requests = list(tr.snapshot.get_range(r.start, r.stop, limit=batch))
	//        r = self._item.range()
	//        items = tr.snapshot.get_range(
	//            r.start, r.stop, limit=batch, reverse=max)

	//        i = 0
	//        for request, (item_key, item_value) in zip(requests, items):
	//            random_ID = self._pop_request.unpack(request.key)[1]
	//            tr[self._requested_item[random_ID]] = item_value
	//            tr.add_read_conflict_key(item_key)
	//            tr.add_read_conflict_key(request.key)
	//            del tr[request.key]
	//            del tr[item_key]
	//            priority, count, _ = self._item.unpack(item_key)
	//            del tr[self._member[item_value][priority][count]]
	//            i += 1

	//        for request in requests[i:]:
	//            tr.add_read_conflict_key(request.key)
	//            del tr[request.key]

	//        tr.commit().wait()
	return nil
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

// Encode & decode may be useless
// func encode(e tuple.TupleElement) []byte {
// 	t := tuple.Tuple{e}
// 	return t.Pack()
// }

// func decode(e []byte) (tuple.Tuple, error) {
// 	return tuple.Unpack(e)
// }
