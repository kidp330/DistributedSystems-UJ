package orient

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"sync"
)

const _idMod = 100000

func _assert(expr bool) {
	if !expr {
		panic("")
	}
}

const ENTROPY = 64 // \in {1..64}

type InPair = [2](<-chan []byte)
type OutPair = [2](chan<- []byte)

type IRState byte

const (
	Active   IRState = iota
	Passive  IRState = iota
	Leader   IRState = iota
	Oriented IRState = iota
)

// region Messages
type MessageType byte

type IMessage interface {
	Marshal() []byte
	Unmarshal(payload []byte)
}

const (
	Election MessageType = iota
	Elected  MessageType = iota
	Collect  MessageType = iota
	Return   MessageType = iota
)

func (mt MessageType) String() string {
	return [...]string{
		"Election",
		"Elected",
		"Collect",
		"Return",
	}[mt]
}

// region struct
type MElection struct {
	id      uint64
	round   uint8
	numHops uint32
	hash    uint32
}

type MElected struct {
}

type MCollect struct {
	kdr int64
}

type MReturn struct {
}

// endregion

// region Marshal
func (msg MElection) Marshal() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, Election)
	binary.Write(buf, binary.LittleEndian, msg.id)
	binary.Write(buf, binary.LittleEndian, msg.round)
	binary.Write(buf, binary.LittleEndian, msg.numHops)
	binary.Write(buf, binary.LittleEndian, msg.hash)
	return buf.Bytes()
}

func (msg MElected) Marshal() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, Elected)
	return buf.Bytes()
}

func (msg MCollect) Marshal() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, Collect)
	binary.Write(buf, binary.LittleEndian, msg.kdr)
	return buf.Bytes()
}

func (msg MReturn) Marshal() []byte {
	return []byte{}
}

// endregion

// region Unmarshal
func (msg_ptr *MElection) Unmarshal(payload []byte) {
	buf := bytes.NewReader(payload)
	var messageType MessageType
	binary.Read(buf, binary.LittleEndian, &messageType) // byte
	// _assert(messageType == Election)                        //debug
	binary.Read(buf, binary.LittleEndian, &msg_ptr.id)      // uint64
	binary.Read(buf, binary.LittleEndian, &msg_ptr.round)   // uint8
	binary.Read(buf, binary.LittleEndian, &msg_ptr.numHops) // uint32
	binary.Read(buf, binary.LittleEndian, &msg_ptr.hash)    // uint32
}

func (msg_ptr *MElected) Unmarshal(payload []byte) {
	buf := bytes.NewReader(payload)
	var messageType MessageType
	binary.Read(buf, binary.LittleEndian, &messageType)
	// _assert(messageType == Elected) //debug
}

func (msg_ptr *MCollect) Unmarshal(payload []byte) {
	buf := bytes.NewReader(payload)
	var messageType MessageType
	binary.Read(buf, binary.LittleEndian, &messageType)
	// _assert(messageType == Collect) //debug
	binary.Read(buf, binary.LittleEndian, &msg_ptr.kdr)
}

func (msg_ptr *MReturn) Unmarshal(payload []byte) {
	_assert(len(payload) == 0)
}

// endregion

func (msg *MElection) getSeenFlag() bool {
	return msg.hash&1 == 1
}

func (msg *MElection) setSeenFlag() {
	msg.hash |= 1
}

// endregion

type RandomBit interface {
	Read() bool
	// Id() uint32 //debug
}

type Candidate struct {
	input  InPair
	output OutPair
	size   uint32
	rng    *rand.Rand

	// Itai Rodeh
	id    uint64
	round uint8
	state IRState

	// Collect
	// kill death ratio
	kdr int64

	// realId uint32 //debug
}

// returns  1 when candidate < msg
// returns -1 when candidate > msg
// returns -1 when candidate = msg
func (candidate *Candidate) cmp(msg MElection) int8 {
	if candidate.round < msg.round {
		return 1
	} else if candidate.round > msg.round {
		return -1
	}
	if candidate.id < msg.id {
		return 1
	} else if candidate.id > msg.id {
		return -1
	}
	return 0
}

func (msg *MElection) setHash(newHash uint32) {
	lastBit := msg.hash & 1
	msg.hash += uint32(newHash)
	msg.hash &= ^uint32(1)
	msg.hash |= lastBit
}

// region interfaces
type ICandidate interface {
	Orientation() bool
}

func InferMessageType(payload []byte) MessageType {
	if len(payload) == 0 {
		return Return
	}
	return MessageType(payload[0])
}

func randInt64(rand RandomBit) int64 {
	var res int64 = 0
	for i := 0; i < 64; i++ {
		if rand.Read() {
			res |= 1 << i
		}
	}
	return res
}

func Orientation(leftInput <-chan []byte, leftOutput chan<- []byte, rightInput <-chan []byte, rightOutput chan<- []byte, size uint64, rngIn RandomBit) bool {
	candidate := NewCandidate(leftInput, leftOutput, rightInput, rightOutput, uint32(size), rngIn)
	return candidate.Orientation()
}

func NewCandidate(leftInput <-chan []byte, leftOutput chan<- []byte, rightInput <-chan []byte, rightOutput chan<- []byte, size uint32, rngIn RandomBit) ICandidate {
	tmp := &Candidate{
		input:  InPair{leftInput, rightInput},
		output: OutPair{leftOutput, rightOutput},
		size:   size,
		rng:    rand.New(rand.NewSource(randInt64(rngIn))),

		round: 0,
		state: Active,

		kdr: 0,

		// realId: rngIn.Id(), //debug
	}
	tmp.id = tmp.rng.Uint64()
	return tmp
}

func (candidate *Candidate) gatherEntropy() {
	var indexWg sync.WaitGroup
	defer indexWg.Wait()

	poke := func(idx byte, start <-chan struct{}) {
		defer indexWg.Done()
		<-start
		candidate.output[idx] <- []byte{}
	}

	seed := uint64(0)
	for i := 0; i < ENTROPY; i++ {
		indexWg.Add(2)

		start := make(chan struct{})
		go poke(0, start)
		go poke(1, start)
		close(start)

		var first byte
		select {
		case <-candidate.input[0]:
			first = 0
		case <-candidate.input[1]:
			first = 1
		}
		<-candidate.input[1-first]
		seed |= uint64(first) << i
	}

	candidate.rng.Seed(
		candidate.rng.Int63() + int64(seed),
	)

	// log.Printf("[id:%05d:%03d] generated seed: %d\n", candidate.id%_idMod, candidate.realId%_idMod, seed) //debug
}

func (candidate *Candidate) Orientation() (shouldFlip bool) {
	candidate.gatherEntropy()

	candidate.id = candidate.rng.Uint64()
	// log.Printf("[id:%05d:%03d] active candidate is sending pebble\n", candidate.id%_idMod, candidate.realId%_idMod) //debug
	candidate.kdr += 1
	candidate.asyncSendPebble()

	var res bool
	for {
		var payload []byte
		var receivedChanIdx byte

		select {
		case payload = <-candidate.input[0]:
			receivedChanIdx = 0
		case payload = <-candidate.input[1]:
			receivedChanIdx = 1
		}
		fwdChanIdx := 1 - receivedChanIdx

		messageType := InferMessageType(payload)
		// log.Printf("[id:%05d:%03d] received msg with type [%s]\n", candidate.id%_idMod, candidate.realId%_idMod, messageType) //debug
		switch messageType {
		case Election:
			candidate.handleMElection(payload, fwdChanIdx)
		case Elected:
			res = candidate.handleMElected(payload, fwdChanIdx)
		case Collect:
			candidate.handleMCollect(payload, fwdChanIdx)
		case Return:
			if candidate.state != Leader {
				candidate.output[fwdChanIdx] <- MReturn{}.Marshal()
			}
			return res
		default:
			panic("unknown msg type")
		}
	}
}

// region Pebble / MElection
func (candidate *Candidate) asyncSendPebble() {
	msg := MElection{
		id:      candidate.id,
		round:   candidate.round,
		numHops: 1,
		hash:    candidate.rng.Uint32() & ^uint32(1),
	}

	go func() {
		candidate.output[1] <- msg.Marshal()
		// log.Printf("[id:%05d:%03d] sendPebble(): success\n", candidate.id%_idMod, candidate.realId%_idMod) //debug
	}()
}

func (candidate *Candidate) asyncFwdPebble(msg MElection, fwdChanIdx byte) {
	msg.numHops += 1
	msg.setHash(candidate.rng.Uint32())
	// candidate.rng.Seed(candidate.rng.Int63() + int64(msg.hash))

	go func() {
		candidate.output[fwdChanIdx] <- msg.Marshal()
		// log.Printf("[id:%05d:%03d] fwdPebble(): success\n", candidate.id%_idMod, candidate.realId%_idMod) //debug
	}()
}

// endregion

// region Elected
func (candidate *Candidate) asyncSendElected() {
	// _assert(candidate.state == Leader) //debug
	go func() {
		candidate.output[1] <- MElected{}.Marshal()
		// log.Printf("[id:%05d:%03d] sendElected(): success\n", candidate.id%_idMod, candidate.realId%_idMod) //debug
	}()
}

func (candidate *Candidate) asyncFwdElected(msg MElected, fwdChanIdx byte) {
	go func() {
		candidate.output[fwdChanIdx] <- msg.Marshal()
		// log.Printf("[id:%05d:%03d] fwdElected(): success\n", candidate.id%_idMod, candidate.realId%_idMod) //debug
	}()
}

// endregion

// region Collect
func (candidate *Candidate) asyncSendCollect() {
	_assert(candidate.state == Leader)
	msg := MCollect{kdr: candidate.kdr}
	go func() {
		candidate.output[1] <- msg.Marshal()
		// log.Printf("[id:%05d:%03d] sendCollect(): success\n", candidate.id%_idMod, candidate.realId%_idMod) //debug
	}()
}

func (candidate *Candidate) asyncFwdCollect(msg MCollect, fwdChanIdx byte) {
	msg.kdr += candidate.kdr
	go func() {
		candidate.output[fwdChanIdx] <- msg.Marshal()
		// log.Printf("[id:%05d:%03d] fwdCollect(): success\n", candidate.id%_idMod, candidate.realId%_idMod) //debug
	}()
}

// endregion

func (candidate *Candidate) handleMElection(payload []byte, fwdChanIdx byte) {
	if candidate.state == Leader || candidate.state == Oriented {
		candidate.kdr -= 1
		return
	}

	var msg MElection
	msg.Unmarshal(payload)
	// log.Printf("[id:%05d:%03d] election message id: [%d]\n", candidate.id%_idMod, candidate.realId%_idMod, msg.id%_idMod) //debug
	// _assert(msg.numHops <= candidate.size)                                                                                //debug

	if candidate.state == Active {
		if msg.numHops == candidate.size {
			// message made a round trip
			if sawIdenticalID := msg.getSeenFlag(); sawIdenticalID {
				// log.Printf("[id:%05d:%03d] detected another node with the same id. changing id...\n", candidate.id%_idMod, candidate.realId%_idMod) //debug

				// reseed using hash
				candidate.rng.Seed(candidate.rng.Int63() + int64(msg.hash))

				// _oldId := candidate.id //debug
				candidate.id = candidate.rng.Uint64()
				candidate.round += 1

				// log.Printf("[id:%05d:%03d] id changed from %05d\n", candidate.id%_idMod, candidate.realId%_idMod, _oldId%_idMod) //debug
				candidate.asyncSendPebble()
			} else {
				candidate.kdr -= 1
				candidate.state = Leader
				// log.Printf("[id:%05d:%03d] candidate has been elected to leader\n", candidate.id%_idMod, candidate.realId%_idMod) //debug
				candidate.asyncSendElected()
			}
		} else {
			switch cmp := candidate.cmp(msg); cmp {
			case 1: // candidate < msg
				candidate.state = Passive
				// log.Printf("[id:%05d:%03d] candidate becomes passive and forwards pebble with id = %05d\n", candidate.id%_idMod, candidate.realId%_idMod, msg.id%_idMod) //debug

				candidate.asyncFwdPebble(msg, fwdChanIdx)
			case -1: // candidate > msg
				// log.Printf("[id:%05d:%03d] candidate blocks message with smaller id = %05d\n", candidate.id%_idMod, candidate.realId%_idMod, msg.id%_idMod) //debug

				candidate.kdr -= 1
			case 0: // candidate == msg
				if msg.numHops < candidate.size {
					// log.Printf("[id:%05d:%03d] msg has the same id but did not originate from candidate\n", candidate.id%_idMod, candidate.realId%_idMod) //debug
					msg.setSeenFlag()

					candidate.asyncFwdPebble(msg, fwdChanIdx)
				} else {
					panic("Branch should be unreachable")
				}
			}
		}
	} else { // candidate.state == Passive
		if msg.numHops < candidate.size {
			// log.Printf("[id:%05d:%03d] passive candidate forwards pebble with id = %05d\n", candidate.id%_idMod, candidate.realId%_idMod, msg.id%_idMod) //debug
			candidate.asyncFwdPebble(msg, fwdChanIdx)
		} else {
			// log.Printf("[id:%05d:%03d] message came back to passive candidate\n", candidate.id%_idMod, candidate.realId%_idMod) //debug
			candidate.kdr -= 1
		}
	}
}

func (candidate *Candidate) handleMElected(payload []byte, fwdChanIdx byte) bool {
	_assert(candidate.state != Oriented)
	var msg MElected
	(&msg).Unmarshal(payload)

	if candidate.state == Leader {
		candidate.asyncSendCollect()
	} else {
		candidate.asyncFwdElected(msg, fwdChanIdx)
		candidate.state = Oriented
	}

	return fwdChanIdx == 0
}

func (candidate *Candidate) handleMCollect(payload []byte, fwdChanIdx byte) {
	var msg MCollect
	(&msg).Unmarshal(payload)

	switch candidate.state {
	case Leader:
		if msg.kdr == 0 {
			// log.Printf("[id:%05d:%03d] leader received Collect with zero kdr, terminating...\n", candidate.id%_idMod, candidate.realId%_idMod) //debug
			candidate.output[fwdChanIdx] <- MReturn{}.Marshal()
		} else {
			// log.Printf("[id:%05d:%03d] leader received Collect with non zero kdr = %d\n", candidate.id%_idMod, candidate.realId%_idMod, msg.kdr) //debug
			candidate.asyncSendCollect()
		}
	// case Oriented:
	default:
		candidate.asyncFwdCollect(msg, fwdChanIdx)
		// panic("Invalid candidate state")
	}
}
