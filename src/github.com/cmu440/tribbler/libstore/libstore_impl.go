package libstore

import (
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net/rpc"
	"sync"
	"time"
)

type storageServer struct {
	node storagerpc.Node
	conn *rpc.Client
}

type valueData struct {
	value  string
	expire int64
}

type listData struct {
	value  []string
	expire int64
}

type libstore struct {
	mutex       sync.Mutex
	libHostPort string
	mode        LeaseMode
	strServers  []storageServer
	queryCache  map[string]([]int64)
	valueCache  map[string](*valueData)
	listCache   map[string](*listData)
	ticker      *time.Ticker
}

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {
	lib, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		return nil, err
	}

	args := &storagerpc.GetServersArgs{}
	var reply storagerpc.GetServersReply
	var strServers []storageServer
	for i := 0; i <= 6; i++ {
		if err := lib.Call("StorageServer.GetServers", args, &reply); err != nil {
			return nil, err
		} else {
			if reply.Status == storagerpc.OK {
				// fine
				strServers = make([]storageServer, 0, len(reply.Servers))
				// fmt.Printf("initial str server's length %d \n", len(strServers))
				// fmt.Printf("Return server's length %d \n", len(reply.Servers))
				for _, val := range reply.Servers {
					connect, err := rpc.DialHTTP("tcp", val.HostPort)
					if err != nil {
						fmt.Println("Error: Fail to connect to storage system!")
						return nil, errors.New("Fail to connect to storage system!")
					}
					// fmt.Printf("Return server's node %d \n", val.NodeID)
					strServers = append(strServers, storageServer{node: val, conn: connect})
				}
			} else {
				if i == 6 {
					fmt.Println("Unable to connect to masterStorageServer")
					return nil, nil
				}
				time.Sleep(time.Second)

			}
		}
	}

	// fmt.Printf("str server's length %d \n", len(strServers))
	myLibstore := &libstore{
		mutex:       sync.Mutex{},
		libHostPort: myHostPort,
		mode:        mode,
		strServers:  strServers,
		queryCache:  make(map[string]([]int64)),
		valueCache:  make(map[string](*valueData)),
		listCache:   make(map[string](*listData)),
		ticker:      time.NewTicker(5 * time.Second),
	}
	// register callback
	if mode != Never {
		rpc.RegisterName("LeaseCallbacks", librpc.Wrap(myLibstore))
	}

	go myLibstore.timeFire()
	return myLibstore, nil
}

func (ls *libstore) Get(key string) (string, error) {
	// fmt.Println("libstore:::inGET")
	var wantLease bool
	if ls.mode == Always {
		wantLease = true
	} else if ls.mode == Never {
		wantLease = false
	} else {
		wantLease = ls.Lease(key)
	}
	ls.mutex.Lock()
	defer ls.mutex.Unlock()

	data, ok := ls.valueCache[key]

	if ok {
		if ls.isValid(data.expire) {
			return data.value, nil
		}
	}

	args := &storagerpc.GetArgs{
		Key:       key,
		WantLease: wantLease,
		HostPort:  ls.libHostPort,
	}
	var reply storagerpc.GetReply
	server := ls.Partition(key)
	if err := server.conn.Call("StorageServer.Get", args, &reply); err != nil {
		return "", err
	}
	if reply.Status != storagerpc.OK {
		return "", errors.New("GetError: Reply status is not OK")
	} else {
		if wantLease && reply.Lease.Granted == wantLease {
			ls.valueCache[key] = &valueData{value: reply.Value, expire: time.Now().UnixNano() + int64(reply.Lease.ValidSeconds)*int64(time.Second)}
		}
		return reply.Value, nil
	}

}

func (ls *libstore) Put(key, value string) error {
	// fmt.Println("libstore:::Put")
	args := &storagerpc.PutArgs{
		Key:   key,
		Value: value,
	}
	var reply storagerpc.PutReply

	server := ls.Partition(key)
	if err := server.conn.Call("StorageServer.Put", args, &reply); err != nil {
		return err
	}
	if reply.Status != storagerpc.OK {
		return errors.New("PutError: Reply status is not OK")
	}
	return nil
}

func (ls *libstore) Delete(key string) error {
	fmt.Println("libstore:::Delete")
	args := &storagerpc.DeleteArgs{
		Key: key,
	}
	var reply storagerpc.DeleteReply
	server := ls.Partition(key)
	if err := server.conn.Call("StorageServer.Delete", args, &reply); err != nil {
		return err
	}
	if reply.Status != storagerpc.OK {
		return errors.New("DeleteError: Reply status is not OK")
	}
	return nil
}

func (ls *libstore) GetList(key string) ([]string, error) {
	// fmt.Println("libstore:::GetList")
	var wantLease bool
	if ls.mode == Always {
		wantLease = true
	} else if ls.mode == Never {
		wantLease = false
	} else {
		wantLease = ls.Lease(key)
	}
	ls.mutex.Lock()
	defer ls.mutex.Unlock()

	data, ok := ls.listCache[key]

	if ok {
		if ls.isValid(data.expire) {
			return data.value, nil
		}
	}

	args := &storagerpc.GetArgs{
		Key:       key,
		WantLease: wantLease,
		HostPort:  ls.libHostPort,
	}
	var reply storagerpc.GetListReply
	server := ls.Partition(key)
	if err := server.conn.Call("StorageServer.GetList", args, &reply); err != nil {
		return nil, err
	}
	if reply.Status != storagerpc.OK {
		return nil, errors.New("GetListError: Reply status is not OK")
	} else {
		if wantLease && reply.Lease.Granted == wantLease {
			ls.listCache[key] = &listData{value: reply.Value, expire: time.Now().UnixNano() + int64(reply.Lease.ValidSeconds)*int64(time.Second)}
		}
		return reply.Value, nil
	}

}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	// fmt.Println("libstore:::RemoveFromList")
	args := &storagerpc.PutArgs{
		Key:   key,
		Value: removeItem,
	}
	var reply storagerpc.PutReply
	server := ls.Partition(key)
	if err := server.conn.Call("StorageServer.RemoveFromList", args, &reply); err != nil {
		return err
	}
	if reply.Status != storagerpc.OK {
		return errors.New("RemoveFromListError: Reply status is not OK")
	}
	return nil
}

func (ls *libstore) AppendToList(key, newItem string) error {
	// fmt.Println("libstore:::AppendToList")
	args := &storagerpc.PutArgs{
		Key:   key,
		Value: newItem,
	}
	var reply storagerpc.PutReply
	server := ls.Partition(key)
	if err := server.conn.Call("StorageServer.AppendToList", args, &reply); err != nil {
		return err
	}
	if reply.Status != storagerpc.OK {
		return errors.New("AppendToListError: Reply status is not OK")
	}
	return nil
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	// fmt.Println("libstore:::RevokeLease")
	ls.mutex.Lock()
	defer ls.mutex.Unlock()
	_, ok := ls.valueCache[args.Key]
	if ok {
		reply.Status = storagerpc.OK
		delete(ls.valueCache, args.Key)
	} else {
		_, ok = ls.listCache[args.Key]
		if ok {
			reply.Status = storagerpc.OK
			delete(ls.listCache, args.Key)
		} else {
			reply.Status = storagerpc.KeyNotFound
		}
	}

	return nil
}

func (ls *libstore) Lease(key string) bool {
	ls.mutex.Lock()
	defer ls.mutex.Unlock()

	query, ok := ls.queryCache[key]

	if ok {
		query = append(query, time.Now().UnixNano())
		// fmt.Printf("fresh query length is  %d \n", len(query))
		// fmt.Println("lease appending")
		old := 0
		for _, value := range query {
			if value+storagerpc.QueryCacheSeconds*int64(time.Second) < time.Now().UnixNano() {
				old++
			}
		}

		// fmt.Printf("old length is %d \n", old)

		query = query[old:]
		ls.queryCache[key] = query

		// fmt.Printf("query length is %d \n", len(query))
	} else {
		queryTime := make([]int64, 0, 1)
		queryTime = append(queryTime, time.Now().UnixNano())
		ls.queryCache[key] = queryTime
	}

	if len(query) >= storagerpc.QueryCacheThresh {
		// fmt.Println("lease appending")
		return true
	} else {
		return false
	}
}

func (ls *libstore) Partition(key string) storageServer {
	// fmt.Printf("total server number is %d \n", len(ls.strServers))
	hashNum := StoreHash(key)

	target := 0
	for index, server := range ls.strServers {

		if server.node.NodeID >= hashNum && (server.node.NodeID < ls.strServers[target].node.NodeID || ls.strServers[target].node.NodeID < hashNum) {
			target = index
		}
	}

	return ls.strServers[target]

}

func (ls *libstore) isValid(expire int64) bool {
	if expire >= time.Now().UnixNano() {
		return true
	} else {
		return false
	}
}

func (ls *libstore) timeFire() {
	for {

		<-ls.ticker.C
		ls.dealCache()

	}
}

func (ls *libstore) dealCache() {
	// fmt.Println("deal with cache")
	ls.mutex.Lock()
	defer ls.mutex.Unlock()

	for key, value := range ls.valueCache {
		if !ls.isValid(value.expire) {
			delete(ls.valueCache, key)
		}
	}

	for key, value := range ls.listCache {
		if !ls.isValid(value.expire) {
			delete(ls.listCache, key)
		}
	}

	for key, value := range ls.queryCache {
		if value[len(value)-1]+storagerpc.QueryCacheSeconds*int64(time.Second) < time.Now().UnixNano() {
			delete(ls.queryCache, key)
		}
	}

}
