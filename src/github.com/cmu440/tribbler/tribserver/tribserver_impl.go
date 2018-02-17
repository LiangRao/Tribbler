package tribserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/util"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"time"
)

type tribServer struct {
	libstore libstore.Libstore
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {

	libstore, err := libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Normal)
	if err != nil {
		fmt.Errorf("Error: ", err)
	}
	tribServer := &tribServer{
		libstore: libstore,
	}

	listener, err := net.Listen("tcp", myHostPort)
	if err != nil {
		return nil, err
	}

	// Wrap the tribServer before registering it for RPC.
	err = rpc.RegisterName("TribServer", tribrpc.Wrap(tribServer))
	if err != nil {
		return nil, err
	}

	// Setup the HTTP handler that will server incoming RPCs and
	// serve requests in a background goroutine.
	rpc.HandleHTTP()
	go http.Serve(listener, nil)

	return tribServer, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	fmt.Println("tribserver:::createUser")

	key := util.FormatUserKey(args.UserID)
	_, err := ts.libstore.Get(key)
	if err == nil {
		reply.Status = tribrpc.Exists
	} else {
		err = ts.libstore.Put(key, key) ///note
		if err != nil {
			return err
		}
		reply.Status = tribrpc.OK
	}
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	fmt.Println("tribserver:::AddSubscription")
	userId := args.UserID
	targetId := args.TargetUserID

	userKey := util.FormatUserKey(userId)
	targetKey := util.FormatUserKey(targetId)
	_, err1 := ts.libstore.Get(userKey)
	_, err2 := ts.libstore.Get(targetKey)
	if err1 != nil {
		reply.Status = tribrpc.NoSuchUser
	} else if err2 != nil {
		reply.Status = tribrpc.NoSuchTargetUser
	} else {
		err := ts.libstore.AppendToList(util.FormatSubListKey(userId), targetId)
		if err != nil {
			reply.Status = tribrpc.Exists
		} else {
			reply.Status = tribrpc.OK
		}
	}
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	fmt.Println("tribserver:::RemoveSubscription")
	userId := args.UserID
	targetId := args.TargetUserID

	userKey := util.FormatUserKey(userId)
	targetKey := util.FormatUserKey(targetId)
	_, err1 := ts.libstore.Get(userKey)
	_, err2 := ts.libstore.Get(targetKey)
	if err1 != nil {
		reply.Status = tribrpc.NoSuchUser
	} else if err2 != nil {
		reply.Status = tribrpc.NoSuchTargetUser
	} else {
		err := ts.libstore.RemoveFromList(util.FormatSubListKey(userId), targetId)
		if err != nil {
			reply.Status = tribrpc.NoSuchTargetUser
		} else {
			reply.Status = tribrpc.OK
		}
	}
	return nil
}

func (ts *tribServer) GetFriends(args *tribrpc.GetFriendsArgs, reply *tribrpc.GetFriendsReply) error {
	fmt.Println("tribserver:::GetFriends")
	userId := args.UserID

	userKey := util.FormatUserKey(userId)
	_, err := ts.libstore.Get(userKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
	} else {
		sublistKey := util.FormatSubListKey(args.UserID)
		subsList, _ := ts.libstore.GetList(sublistKey)
		friendList := make([]string, 0)

		for i := 0; i < len(subsList); i++ {
			currentSubKey := util.FormatSubListKey(subsList[i])
			currentList, _ := ts.libstore.GetList(currentSubKey)
			if contains(currentList, userId) {
				friendList = append(friendList, subsList[i])
			}
		}

		reply.UserIDs = friendList
		reply.Status = tribrpc.OK

	}
	return nil
}

func contains(subsList []string, userId string) bool {

	for _, a := range subsList {
		if a == userId {
			return true
		}
	}
	return false
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	fmt.Println("tribserver:::PostTribble")
	userId := args.UserID

	userKey := util.FormatUserKey(userId)
	_, err := ts.libstore.Get(userKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
	} else {
		postTime := time.Now().UTC()
		tribble := &tribrpc.Tribble{
			UserID:   args.UserID,
			Posted:   postTime,
			Contents: args.Contents,
		}
		tribbleByte, _ := json.Marshal(tribble)
		postKey := util.FormatPostKey(userId, postTime.UnixNano())
		err = ts.libstore.Put(postKey, string(tribbleByte))
		if err != nil {
			return errors.New("Fail to put postKey into database")
		}
		tribKey := util.FormatTribListKey(userId)
		err = ts.libstore.AppendToList(tribKey, postKey)
		if err != nil {
			return errors.New("Fail to append tribble")
		}
		reply.Status = tribrpc.OK
		reply.PostKey = postKey
	}
	return nil
}

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	fmt.Println("tribserver:::DeleteTribble")
	userId := args.UserID

	userKey := util.FormatUserKey(userId)
	_, err := ts.libstore.Get(userKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
	} else {

		err = ts.libstore.Delete(args.PostKey)
		if err != nil {
			reply.Status = tribrpc.NoSuchPost
			return nil
		}
		tribKey := util.FormatTribListKey(args.UserID)
		err = ts.libstore.RemoveFromList(tribKey, args.PostKey)
		if err != nil {
			reply.Status = tribrpc.NoSuchPost
			return nil
		}
		reply.Status = tribrpc.OK
	}
	return nil
}

// func (ts *tribServer) GetTribblesContent(userId string) ([]tribrpc.Tribble, error) {
// 	tribkey := util.FormatTribListKey(userId)
// 	tribIdList, err_gl := ts.libstore.GetList(tribkey)
// 	if err_gl != nil {
// 		return nil, errors.New("Fail to get tribblers of a user")
// 	}
// 	length := len(tribIdList)
// 	if length > 100 {
// 		length = 100
// 	}
// 	tribList := make([]tribrpc.Tribble, 0, length)

// 	for i := 0; i < length; i++ {

// 		postKey := tribIdList[i]
// 		tribbleByte, err_g := ts.libstore.Get(postKey)
// 		if err_g != nil {
// 			continue ///note
// 		}
// 		var tribble tribrpc.Tribble
// 		json.Unmarshal([]byte(tribbleByte), &tribble)
// 		tribList = append(tribList, tribble)
// 	}

// 	return tribList, nil
// }
func (ts *tribServer) GetTribblesContent(userId string) []tribrpc.Tribble {
	// fmt.Println("tribserver:::GetTribblesContent")
	tribkey := util.FormatTribListKey(userId)
	tribIdList, err := ts.libstore.GetList(tribkey)
	if err != nil {
		return nil
	}

	var length int
	if len(tribIdList) > 100 {
		length = 100
	} else {
		length = len(tribIdList)
	}
	tribList := make([]tribrpc.Tribble, 0, length)
	var tribble tribrpc.Tribble
	for i := 0; i < length; i++ {
		tribId := tribIdList[len(tribIdList)-1-i]
		tribbleString, _ := ts.libstore.Get(tribId)
		json.Unmarshal([]byte(tribbleString), &tribble)
		tribList = append(tribList, tribble)
	}

	return tribList
}
func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	fmt.Println("tribserver:::GetTribbles")
	userId := args.UserID

	userKey := util.FormatUserKey(userId)
	_, err := ts.libstore.Get(userKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
	} else {
		reply.Status = tribrpc.OK
		tribList := ts.GetTribblesContent(userId)
		if tribList != nil {
			reply.Tribbles = tribList
		}
	}

	return nil
}

type TribbleSlice []tribrpc.Tribble

func (tribbleSlice TribbleSlice) Len() int {
	return len(tribbleSlice)
}
func (tribbleSlice TribbleSlice) Swap(i, j int) {
	tribbleSlice[i], tribbleSlice[j] = tribbleSlice[j], tribbleSlice[i]
}
func (tribbleSlice TribbleSlice) Less(i, j int) bool {
	return tribbleSlice[i].Posted.After(tribbleSlice[j].Posted)
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	fmt.Println("tribserver:::GetTribblesBySubscription")
	userId := args.UserID

	userKey := util.FormatUserKey(userId)
	_, err := ts.libstore.Get(userKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchUser
	} else {
		reply.Status = tribrpc.OK
		sublistKey := util.FormatSubListKey(args.UserID)
		subsList, _ := ts.libstore.GetList(sublistKey)

		tribList := make(TribbleSlice, 0)
		for i := 0; i < len(subsList); i++ {
			currentUserId := subsList[i]
			currentTribList := ts.GetTribblesContent(currentUserId)
			tribList = append(tribList, currentTribList...)
		}
		sort.Sort(tribList)
		var totalLength int
		if len(tribList) > 100 {
			totalLength = 100
		} else {
			totalLength = len(tribList)
		}
		reply.Tribbles = tribList[0:totalLength]

	}
	return nil
}
