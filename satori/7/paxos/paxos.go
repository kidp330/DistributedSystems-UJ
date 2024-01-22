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

// region MaxKey
// region InstanceState
func MaxKey(m map[int]InstanceState) int {
	mx := new(int)
	for k := range m {
		if mx == nil || k > *mx {
			mx = &k
		}
	}
	if mx == nil {
		return -2147483648
	}
	return *mx
}

// endregion
// endregion

// region Supervisor
type Supervisor struct {
	quit chan struct{}
	wg   *sync.WaitGroup
}

func NewSupervisor() *Supervisor {
	return &Supervisor{quit: make(chan struct{}), wg: &sync.WaitGroup{}}
}

func (s *Supervisor) Start(tasks ...func()) {
	for _, task := range tasks {
		s.wg.Add(1)
		go func(t func()) {
			t()
			s.wg.Done()
		}(task)
	}
}

func (s *Supervisor) SignalStop() {
	close(s.quit)
}

func (s *Supervisor) Stop() {
	s.SignalStop()
	s.wg.Wait()
}

// endregion

// region Order
type TypeMapping byte

const (
	t_string TypeMapping = iota
	t_int    TypeMapping = iota
)

type OrderedType = interface{}

func Order(ov OrderedType) TypeMapping {
	switch ov.(type) {
	case int:
		return t_int
	case string:
		return t_string
	default:
		panic("")
	}
}

// endregion

// region comparable
type ComparablePair interface {
	Equal() bool
	LessThan() bool
	LessOrEq() bool
	GreaterThan() bool
	GreaterOrEq() bool
}

type InterfacePair struct {
	first       interface{}
	second_dupa interface{}
}

func (p *InterfacePair) Swap() *InterfacePair {
	return &InterfacePair{
		first:       p.second_dupa,
		second_dupa: p.first,
	}
}

// region comparisons
func (p *InterfacePair) Equal() bool {
	return !(p.LessThan() || p.GreaterThan())
}

func (p *InterfacePair) LessOrEq() bool {
	return !p.GreaterThan()
}

func (p *InterfacePair) GreaterThan() bool {
	return p.Swap().LessThan()
}

func (p *InterfacePair) GreaterOrEq() bool {
	return !p.LessThan()
}

func (p *InterfacePair) LessThan() bool {
	t1 := Order(p.second_dupa)
	switch t2 := Order(p.first); {
	case t1 < t2:
		return true
	case t1 > t2:
		return false
	}

	switch t1 { // __jm__ wonder if thisll work for interface{} already cast to OrderedType
	case t_string:
		return p.first.(string) < p.second_dupa.(string)
	case t_int:
		return p.first.(int) < p.second_dupa.(int)
	default: // debug
		panic("unknown type") // debug
	}
}

// endregion

// endregion

// region Consensus DSL
// functionally an alias, overwritten for type checks
type ConsensusValue interface{} // int | string

type Consensus struct{}

func (_ Consensus) Compare(a, b ConsensusValue) InterfacePair {
	return InterfacePair{
		first:       a,
		second_dupa: b,
	}
}

// endregion

// region Messages
type Message = interface{}
type Response = Message

// Could really use a hierarchy tree here

// in a typical scenario a reply to a phase 1 proposal
// is peer id + the last promise number or -1 if not yet set
// another scenario is the peer already having accepted some consensus value
// which it forwards to our proposer, who is now required to reach consensus with this value
type ResponseWithId struct {
	id       int
	response Response
}

type PaxosMessage struct {
	maxDone int
	payload Message
}

// send phase 1:
// seq
// uid
// [optional] piggyback maxDone
type MessageP1 struct {
	seq int
	uid int
}

// reply phase 1:
// empty (NACK)
// or
// ok + last accepted value, if any
// [optional] piggyback maxDone
type ResponseP1 struct {
	accepted *ConsensusValue
}

// send phase 2
// seq
// uid
// consensus value
// [optional] piggyback maxDone
type MessageP2 struct {
	seq   int
	uid   int
	value ConsensusValue
}

// reply phase 2
// empty (NACK)
// or
// ok
// if the acceptor already previously accepted some value,
// it must be the same as the value received now. (debug eq check)
// [optional] piggyback maxDone
type ResponseP2 struct {
}

// endregion

// region Paxos struct
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

// region State
type ProposerState struct {
	supervisor Supervisor
	uid        int
}

// region AcceptorState
type AcceptorState struct {
	curr        ConsensusValue
	maxPromised *int
}

func (as AcceptorState) isDecided() (bool, ConsensusValue) {
	if as.curr == nil {
		return false, nil
	}
	return true, as.curr
}

// endregion

type InstanceState struct {
	proposer *ProposerState
	acceptor AcceptorState
}

// endregion

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

	maxDone         int
	instanceToState map[int]InstanceState
}

func (px *Paxos) Init() {
	px.maxDone = -1
	px.instanceToState = make(map[int]InstanceState)
}

// endregion

// region call (ignore)
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

// endregion

// this function should RPC call methodName asynchronously on all peers
// it spawns a single goroutine for each peer, which ends only in two cases:
//   - receiving a response from peer
//   - receiving a quit signal
func (px *Paxos) sendToAll(
	quit <-chan struct{},
	responses chan<- ResponseWithId,
	methodName string,
	buildMessageCallback func(int) PaxosMessage, // MessageP1 | MessageP2
) {

	type ResponseWithStatus = struct {
		ok       bool
		response PaxosMessage
	}

	singleSend := func(
		idx int, peer string,
		responseChan chan<- ResponseWithStatus,
	) {
		args := buildMessageCallback(idx)
		var response PaxosMessage

		ok := call(peer, methodName, args, &response)
		responseChan <- ResponseWithStatus{ok, response}
	}

	// this builds a closure so our function has its args already provided
	taskBuilder := func(
		idx int, peer string,
		responses chan<- ResponseWithId,
		quit <-chan struct{},
	) func() {
		sendRepeatOnFailure := func() {
			sendResponseChan := make(chan ResponseWithStatus)
			for {
				go singleSend(idx, peer, sendResponseChan)
				select {
				// TODO: this needs a better flow, rn it quits before the sender times out
				case <-quit:
					fallthrough
				case respWithStatus := <-sendResponseChan:
					if respWithStatus.ok {
						// responses <- idResponse{idx, respWithStatus.response}
						responses <- ResponseWithId{
							id:       idx,
							response: respWithStatus.response,
						}
						return
					} // else timeout, repeat
				}
			}
		}

		return sendRepeatOnFailure
	}

	s := NewSupervisor()
	tasks := make([]func(), 0)

	for i, peer := range px.peers {
		var task func()
		task = taskBuilder(i, peer, responses, s.quit)
		tasks = append(tasks, task)
	}

	s.Start(tasks...)
	<-quit
	s.Stop()
}

// wrapper around `sendToAll`
func (px *Paxos) sendPhase1(
	seq int,
	quit <-chan struct{},
	// responseChan chan<- idResponse,
	responseChan chan<- ResponseWithId,
) {
	px.mu.Lock()
	maxDone := px.maxDone
	uid := px.instanceToState[seq].proposer.uid
	px.mu.Unlock()

	px.sendToAll(quit, responseChan, "Paxos.RPC_Ask",
		func(idx int) PaxosMessage {
			return PaxosMessage{
				maxDone: maxDone,
				payload: MessageP1{
					seq: seq,
					uid: uid,
				},
			} // __jm__ TODO: seq with uid
		},
	)
}

type BreakReason byte

const (
	TimedOut     BreakReason = iota
	None         BreakReason = iota
	ParentQuit   BreakReason = iota
	MajorityAck  BreakReason = iota
	MajorityNack BreakReason = iota
)

func (px *Paxos) receivePhase1(
	// promiseResponses <-chan idResponse,
	seq int,
	proposerQuit <-chan struct{},
	responses <-chan Message,
) BreakReason {

	promiseAcks := 0
	promiseNacks := 0
	majority := len(px.peers)/2 + 1

	// handlePromiseResponse := func(responseWithId idResponse) {
	handlePromiseResponse := func(responseWithId Message) {
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
	for start := time.Now(); time.Since(start) < TIMEOUT_WAITING_PROMISES ||
		promiseAcks >= majority ||
		promiseNacks >= majority; {

		select {
		case responseWithId := <-responses:
			handlePromiseResponse(responseWithId)
		case <-proposerQuit:
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

type ProposerSendCallback = func(seq int, quit <-chan struct{}, responseChan chan<- Message)
type ProposerReceiveCallback = func(seq int, quit <-chan struct{}, responseChan <-chan Message) BreakReason

func (px *Paxos) proposerSendReceive(seq int, send ProposerSendCallback, receive ProposerReceiveCallback) BreakReason {
	rootQuit := px.instanceToState[seq].proposer.supervisor.quit
	sendersSupervisor := NewSupervisor()
	// responseChan := make(chan idResponse)
	responseChan := make(chan Message)
	sendersSupervisor.Start(func() {
		send(seq, rootQuit, responseChan)
	})
	// receive polls the response channel continuously until a majority of promises is received
	// or some other condition is met - in particular a signal is sent to proposerQuit
	result := receive(seq, rootQuit, responseChan)
	sendersSupervisor.Stop()
	return result
}

func (px *Paxos) proposerLoop(seq int) {
	phase, reason := 0, None
	for {
		switch {
		case reason == None:
			fallthrough
		case reason == TimedOut:
			phase, reason = 1, px.proposerPhase1(seq)
		case reason == ParentQuit:
			return ParentQuit
		case reason == MajorityAck && phase == 1:
			phase, reason = 2, px.proposerPhase2(seq)
		case reason == MajorityAck && phase == 2:
			return MajorityAck
		case reason == MajorityNack:
			return MajorityNack
		default:
			panic("Unexpected transition in proposer loop")
		}
	}
}

func (px *Paxos) proposerPhase1(seq int) BreakReason {
	rootQuit := px.instanceToState[seq].proposer.supervisor.quit
	sendersSupervisor := NewSupervisor()
	// responseChan := make(chan idResponse)
	responseChan := make(chan Message)
	sendersSupervisor.Start(func() {
		px.sendPhase1(seq, sendersSupervisor.quit, responseChan)
	})
	// receivePhase1 polls the response channel continuously until a majority of promises is received
	// or some other condition is met - in particular a signal is sent to proposerQuit
	result := px.receivePhase1(responseChan, seq, rootQuit)
	sendersSupervisor.Stop()
	return result
}

// __jm__ proposer <-> acceptor opts on the same thread should be handled and communicated by a separate thread

// proposer gives up when:
// * it receives a rejection from the majority
// * it promises to a proposer with a higher sequence number -- it is killed by the parent node's acceptor thread
func (px *Paxos) proposerPhase2(seq int) BreakReason {
	rootQuit := px.instanceToState[seq].proposer.supervisor.quit
	sendersSupervisor := NewSupervisor()
	// responseChan := make(chan idResponse)
	responseChan := make(chan Message)
	sendersSupervisor.Start(func() {
		px.sendPhase2(seq, sendersSupervisor.quit, responseChan)
	})
	// receivePhase1 polls the response channel continuously until a majority of promises is received
	// or some other condition is met - in particular a signal is sent to proposerQuit
	result := px.receivePhase2(responseChan, seq, rootQuit)
	sendersSupervisor.Stop()
	return result
}

// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
func (px *Paxos) Start(seq int, v interface{}) {
	go func() {
		// cleanup previous proposer thread if applicable
		if px.isProposer(seq) {
			px.instanceToState[seq].proposer.supervisor.Stop()
		}
		if _, ok := px.instanceToState[seq]; !ok {
			// px.instanceToState[seq] = newInstanceState(seq, v)
		}
		// generate uid, init proposerState
		// overwrite old proposerState
		// px.instanceToState[seq].proposer = newProposer(seq)
		px.proposerLoop(seq)
	}()
}

// region RPC
func (px *Paxos) RPC_Phase1(msg Message, reply Message) error {
	px.mu.Lock()
	if px.promise == nil || *px.promise < seq {
		px.promise = &seq
	}
	*reply = *px.promise
	px.mu.Unlock()
	return nil
}

// endregion

// region Done
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
func (px *Paxos) Done(seq int) {
	// peer advertises seq to both proposer and acceptor to include it in the exchanged messages
	px.mu.Lock()
	px.maxDone = seq
	for key_seq, state := range px.instanceToState {
		if key_seq <= seq {
			state.SignalStop()
		}
	}
	// TODO: wait for instances
	// cleanup instances <= seq
	// set val to be known to instances > seq
	// to minimalise time spent locked,
	// implement a peer-scoped thread listening to Done calls, and signaling the appropriate instances
	// or maybe have the thread reply to this thread when the work is finished?
	px.mu.Unlock()
	// The question to answer here is whether it's important to return from this call quickly. If yes, the work needs to be delegated to a separate thread supervised by peer
}

func (px *Paxos) safeMaxDone() int {
	px.mu.Lock()
	res := px.maxDone
	px.mu.Unlock()
	return res
}

// endregion

// region Max
// the application wants to know the
// highest instance sequence known to
// this peer.
// __jm__
// this should be enough
func (px *Paxos) Max() int {
	px.mu.Lock()
	its := px.instanceToState
	px.mu.Unlock()
	res := MaxKey(its)
	return res
}

// endregion

// region Min
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

// endregion

// region Status
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
// __jm__
// don't check if is proposer, treat a node as running
// both an acceptor and proposer thread
func (px *Paxos) Status(seq int) (bool, interface{}) {
	px.mu.Lock()
	state, ok := px.instanceToState[seq]
	px.mu.Unlock()
	if ok {
		return state.acceptor.isDecided()
	}
	return false, nil
}

// endregion

// region Kill (ignore)
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

// endregion

// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me
	// Your initialization code here.
	px.Init()
	//
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
