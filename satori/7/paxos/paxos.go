package paxos

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"syscall"
	"time"
)

const TIMEOUT_WAITING_PROMISES = time.Second // __jm__
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int
	// index into peers[]
	// Your data here.
	promise *int

	promises    []bool
	numPromises int

	proposerQuit chan struct{}
}

// region message structs
type startMsg struct {
	seq int
}

// endregion

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
// call (peer_me, peer,. ...)
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Fprintf(os.Stderr, "paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()
	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}
	fmt.Fprintln(os.Stderr, err)
	return false
}

// in a typical scenario a reply to a phase 1 proposal
// is peer id + the last promise number or -1 if not yet set
// another scenario is the peer already having accepted some consensus value
// which it forwards to our proposer, who is now required to reach consensus with this value
type idResponse = struct {
	id       int
	response interface{}
}

// possible types of (response interface{}) :
// TODO __jm__

// this function should RPC call methodName asynchronously on all peers
// it spawns a single goroutine for each peer, which ends only in two cases:
//   - receiving a response from peer
//   - receiving a quit signal
func (px *Paxos) sendToAll(
	quit <-chan struct{},
	responses chan<- idResponse,
	methodName string,
	getArgsCallback func(int) interface{}) {

	for i, peer := range px.peers {
		if i == px.me {
			continue
		}

		type responseWithStatus = struct {
			ok       bool
			response interface{}
		}

		singleSend := func(
			idx int, peer string,
			responseChan chan<- responseWithStatus,
		) {
			args := getArgsCallback(idx)
			response := new(interface{})

			ok := call(peer, methodName, args, &response)
			responseChan <- responseWithStatus{ok, response}
		}

		sendRepeatOnFailure := func(
			idx int, peer string,
			responses chan<- idResponse,
			quit <-chan struct{},
		) {
			sendResponseChan := make(chan responseWithStatus)
			for {
				go singleSend(idx, peer, sendResponseChan)
				select {
				case respWithStatus := <-sendResponseChan:
					if respWithStatus.ok {
						responses <- idResponse{idx, respWithStatus.response}
						return
					} // else timeout, repeat
				case <-quit:
					return
				}
			}
		}

		go sendRepeatOnFailure(i, peer, responses, quit)
	}
}

func (px *Paxos) askPromises(seq int, quit <-chan struct{}) <-chan idResponse {
	promiseResponses := make(chan idResponse)
	px.sendToAll(quit, promiseResponses, "Paxos.RPC_Ask",
		func(idx int) interface{} {
			return seq
		},
	)
	return promiseResponses
}

type BreakReason byte

const (
	TimedOut     BreakReason = iota
	None         BreakReason = iota
	ParentQuit   BreakReason = iota
	MajorityAck  BreakReason = iota
	MajorityNack BreakReason = iota
)

func (px *Paxos) gatherPromises(promiseResponses <-chan idResponse, seq int, proposerQuit <-chan struct{}) BreakReason {
	promiseAcks := 0
	promiseNacks := 0
	majority := len(px.peers)/2 + 1

	handlePromiseResponse := func(responseWithId idResponse) {
		promiseSeq := *responseWithId.response.(*int)
		switch {
		// received response is a promise
		case promiseSeq == -1 || promiseSeq < seq:
			promiseAcks++
		case promiseSeq > seq:
			promiseNacks++
		case promiseSeq == seq: // debug
			panic("Something's not right") // debug
		}
	}

	// three cases:
	// 	- success due to acks >= majority
	//  - failure due to nacks >= majority
	//  - failure due to timeout, reset and send proposals again
	for start := time.Now(); promiseAcks >= majority ||
		promiseNacks >= majority ||
		time.Since(start) < TIMEOUT_WAITING_PROMISES; {

		select {
		case responseWithId := <-promiseResponses:
			handlePromiseResponse(responseWithId)
		case <-proposerQuit:
			// cleanup?
			return ParentQuit
		}
	}

	switch {
	case promiseAcks >= majority:
		return MajorityAck
	case promiseNacks >= majority:
		return MajorityNack
	default:
		return TimedOut
	}
}

// proposer gives up when:
// * it receives a rejection from the majority
// * it promises to a proposer with a higher sequence number
func (px *Paxos) proposer(seq int, v interface{}) {
	{ // Phase 1
		lastIterResult := None
		for lastIterResult == TimedOut ||
			lastIterResult == None {
			// quit := make(chan struct{})
			promiseResponses := px.askPromises(seq, px.proposerQuit)
			lastIterResult = px.gatherPromises(promiseResponses, seq, px.proposerQuit)
		}

		switch lastIterResult {
		case MajorityAck:
			// no-op, continue
		case MajorityNack:
			close(px.proposerQuit)
			return
		case ParentQuit:
			return
		default: // debug
			panic("Something went wrong") // debug
		}
	}

	{ // Phase 2
		lastIterResult := None
		for lastIterResult {

		}
	}
}

// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
func (px *Paxos) Start(seq int, v interface{}) {
	go px.proposer(seq, v)

}

// region RPC
func (px *Paxos) RPC_Ask(seq int, reply *int) error {
	px.mu.Lock()
	if px.promise == nil || *px.promise < seq {
		px.promise = &seq
	}
	*reply = *px.promise
	px.mu.Unlock()
	return nil
}

// endregion

// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
func (px *Paxos) Done(seq int) {
	// Your code here.
}

// the application wants to know the
// highest instance sequence known to
// this peer.
func (px *Paxos) Max() int { return 0 }

// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
func (px *Paxos) Min() int { return 0 }

// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
func (px *Paxos) Status(seq int) (bool, interface{}) {
	// Your code here.
	return false, nil
}

// tell the peer to shut itself down.
// for testing.
// please do not change this function.
func (px *Paxos) Kill() {
	log.Println("killed")
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me
	// Your initialization code here.
	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)
		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me])
		// only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}
	return px
}
