package king

import (
	"bytes"
	"encoding/binary"
	"math/rand"
)

func max_int(a, b int) int {
	if a < b {
		return b
	}
	return a
}

func Marshal(msg Message) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, msg.MessageType)
	binary.Write(buf, binary.LittleEndian, msg.Id)
	binary.Write(buf, binary.LittleEndian, int64(msg.AnsId))
	return buf.Bytes()
}

func Unmarshal(payload []byte, msg_ptr *Message) {
	buf := bytes.NewReader(payload)
	var tmp int64
	binary.Read(buf, binary.LittleEndian, &msg_ptr.MessageType)
	binary.Read(buf, binary.LittleEndian, &msg_ptr.Id)
	binary.Read(buf, binary.LittleEndian, &tmp)
	msg_ptr.AnsId = int(tmp)
}

type MessageType = byte

const (
	Election MessageType = iota
	Elected  MessageType = iota
)

type Message struct {
	MessageType MessageType
	Id          uint64
	AnsId       int
}

type Candidate struct {
	Input   <-chan []byte
	Output  chan<- []byte
	Id      uint64
	OgId    int
	MaxSeen uint64
}

type ICandidate interface{ SelectLeader() int }

func NewCandidate(id int, input <-chan []byte, output chan<- []byte) ICandidate {
	tmp := &Candidate{
		Input:  input,
		Output: output,
		Id:     rand.Uint64(),
		OgId:   id,
	}
	tmp.MaxSeen = tmp.Id
	return tmp
}

func (candidate *Candidate) SelectLeader() int {
	go func() {
		payload := Marshal(Message{
			MessageType: Election,
			Id:          candidate.Id,
			AnsId:       candidate.OgId,
		})
		candidate.Output <- payload
	}()
	for {
		var msg Message
		recBuffer := <-candidate.Input
		Unmarshal(recBuffer, &msg)

		switch msg.MessageType {
		case Election:
			candidate.handleMElection(msg)
		case Elected:
			return candidate.handleMElected(msg)
		default:
			panic("Unknown message code")
		}
	}
}

func (candidate *Candidate) handleMElection(msg Message) {
	if candidate.MaxSeen < msg.Id {
		candidate.MaxSeen = msg.Id
		msg.AnsId = max_int(msg.AnsId, candidate.OgId)
		payload := Marshal(msg)
		candidate.Output <- payload
	} else if candidate.Id == msg.Id {
		payload := Marshal(Message{
			MessageType: Elected,
			Id:          candidate.Id,
			AnsId:       msg.AnsId,
		})
		candidate.Output <- payload
	}
}

func (candidate *Candidate) handleMElected(msg Message) int {
	if candidate.Id != msg.Id {
		payload := Marshal(msg)
		candidate.Output <- payload
	}

	return msg.AnsId
}
