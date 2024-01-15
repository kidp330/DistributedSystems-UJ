package main

import (
	"encoding/json"
)

type IServer interface {
	getTime(Sender *SSender) int
	sendTo(Sender *SSender, ReceiverId int, data []byte)
}

type SSender struct {
	Id              int
	NumberOfSenders int
	Times           []int
	server          IServer
	data            interface{}
}

func NewSender(id int, numberOfSenders int, server IServer) *SSender {
	Sender := new(SSender)
	Sender.Id = id
	Sender.NumberOfSenders = numberOfSenders
	Sender.server = server
	Sender.Times = make([]int, numberOfSenders)
	for i := range Sender.Times {
		Sender.Times[i] = 0
	}
	return Sender
}

type Message struct {
	VectorClock []int
}

func (Sender *SSender) send(receiverId int) {
	time := Sender.server.getTime(Sender)
	Sender.Times[Sender.Id] = time

	data, err := json.Marshal(Message{
		VectorClock: Sender.Times,
	})
	if err != nil {
		// fmt.Println(Sender)
	}

	Sender.server.sendTo(Sender, receiverId, data)
}

// true: proces zarejestrował już zdarzenie `B`, które wydarzyło się `później` niż aktualnie przetwarzane zdarzenie `A`, tj. `A` --> `B`
// false: w przeciwnym wypadku
// tzn.: A nie musi (nie bedzie, bo inkrementowany koordynat wysylajacego procesu sie nie powtorzy) byc bezposrednim srodkiem B
// np. Jezeli Sender.Times[m.id] >= m.Times[m.id]
// return true when I;ve already seen something that definitely happened after received
func (Sender *SSender) receive(data []byte) bool {
	time := Sender.server.getTime(Sender)

	var m Message
	if err := json.Unmarshal(data, &m); err != nil {
		// fmt.Println(Sender, data)
	}

	// can sender send to itself?
	// flag = VC(m) ⪯ VC(S) and (exists i. VC(m)[i] < VC(S)[i])
	maj := true
	existsStrictlyGreater := false
	for i, t := range m.VectorClock {
		if t > Sender.Times[i] {
			Sender.Times[i] = t
			maj = false
		} else if t < Sender.Times[i] {
			existsStrictlyGreater = true
		}
	}
	Sender.Times[Sender.Id] = time

	return maj && existsStrictlyGreater
}

//
// func main() {}
