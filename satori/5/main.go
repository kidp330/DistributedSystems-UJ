package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"

	"satori.tcs.uj.edu.pl/m/v2/orient"
)

type randBit struct {
	r  *rand.Rand
	id uint32
}

func (rand *randBit) Read() bool {
	return rand.r.Intn(2) == 0
}

func (rand *randBit) Id() uint32 {
	return rand.id
}

func closeChannel(channel chan []byte) {
	for len(channel) > 0 {
	}
	close(channel)
}
func newRandomBit(id uint32) orient.RandomBit {
	return &randBit{
		r:  rand.New(rand.NewSource(1)),
		id: id,
	}
}

type NullWriter struct{}

func (NullWriter) Write([]byte) (int, error) { return 0, nil }
func main() {
	rc := 0b0000
	{
		f, err := os.Create("test.prof")
		if err != nil {
			panic(err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			panic(err)
		}
		defer pprof.StopCPUProfile()

		log.SetOutput(os.Stderr) // NOTE: it is throwing exceptions since I am closing channels that have non-empty buffers
		const bufferSize = 0
		const internalBufferSize = 0
		runtime.GOMAXPROCS(1)

		var N uint64
		fmt.Scanf("%d\n", &N)

		var messageSizeLimit uint64
		fmt.Scanf("%d\n", &messageSizeLimit)

		var messageCountLimit uint64
		fmt.Scanf("%d\n", &messageCountLimit)

		var wg sync.WaitGroup
		var channelWg sync.WaitGroup
		resultChannel := make(chan bool, internalBufferSize)

		runner := func(value uint64, flip bool, leftInput, leftOutput chan []byte, rightInput chan []byte, rightOutput chan []byte, randomBit orient.RandomBit) {
			defer wg.Done()
			defer channelWg.Done()

			result := orient.Orientation(leftInput, leftOutput, rightInput, rightOutput, value, randomBit)
			if result {
				flip = !flip
			}
			resultChannel <- flip
		}

		type MessageInfo struct {
			MessageSize  uint64
			MessageCount uint64
		}

		dispatchChannel := make(chan []byte, internalBufferSize)
		messageSizeChannel := make(chan MessageInfo, internalBufferSize)
		go func(dispatchChannel chan []byte) {
			var messageSize, messageCount uint64 = 0, 0
			for message := range dispatchChannel {
				messageCount++
				messageSize += uint64(len(message))
			}

			messageSizeChannel <- MessageInfo{messageSize, messageCount}
		}(dispatchChannel)

		nodeDispatcher := func(output chan []byte, input chan []byte, dispatchChannel chan []byte) {
			defer wg.Done()
			var message []byte
			defer func() {
				if r := recover(); r != nil {
					rc |= 0b0001
					fmt.Println("ERROR: recovering...last message ", message)
				}
			}()

			for message = range output {

				copied := make([]byte, len(message))
				copy(copied, message)
				dispatchChannel <- message // NOTE: exception at this point - send on closed channel
				input <- copied
			}
		}
		firstInput1, firstInput2 := make(chan []byte, bufferSize), make(chan []byte, bufferSize)
		firstOutput1, firstOutput2 := make(chan []byte, bufferSize), make(chan []byte, bufferSize)
		channels := [](chan []byte){firstOutput1, firstOutput2, firstInput1, firstInput2}
		prevOutput := firstOutput2
		prevInput := firstInput2
		for it := uint64(0); it < N; it++ { // TODO: change this to string
			var orientation bool
			fmt.Scanf("%t\n", &orientation)
			var leftInput chan []byte = prevOutput
			var leftOutput chan []byte = prevInput
			var rightOutput1, rightOutput2 chan []byte
			var rightInput1, rightInput2 chan []byte
			if it == N-1 {
				rightOutput1, rightOutput2 = firstOutput1, firstOutput2
				rightInput1, rightInput2 = firstInput1, firstInput2 // N = -N
			} else {
				rightOutput1, rightOutput2 = make(chan []byte, bufferSize), make(chan []byte, bufferSize)
				rightInput1, rightInput2 = make(chan []byte, bufferSize), make(chan []byte, bufferSize)
				channels = append(channels, rightInput1, rightInput2, rightOutput1, rightOutput2)
			}

			randomBit := newRandomBit(uint32(it))
			leftInputTmp, leftOutputTmp := leftInput, leftOutput
			rightInputTmp, rightOutputTmp := rightInput1, rightOutput1
			if !orientation {
				leftInputTmp = rightInput1
				leftOutputTmp = rightOutput1
				rightInputTmp = leftInput
				rightOutputTmp = leftOutput
			}

			wg.Add(3)
			channelWg.Add(1)
			go nodeDispatcher(rightOutput1, rightOutput2, dispatchChannel)
			go nodeDispatcher(rightInput2, rightInput1, dispatchChannel)
			go runner(N, orientation, leftInputTmp, leftOutputTmp, rightInputTmp, rightOutputTmp, randomBit)

			prevOutput = rightOutput2
			prevInput = rightInput2

			log.Println("node created")
		} // N = -N
		tmp := false
		lastResult := &tmp
		lastResult = nil
		for it := uint64(0); it < N; it++ {
			flipValue := <-resultChannel
			if lastResult != nil {
				if *lastResult != flipValue {
					rc |= 0b0010
					fmt.Println("failed flipping")
				}
			}
			lastResult = &flipValue
		}

		log.Println("received all results")
		channelWg.Wait()
		log.Println("closing channels...")
		for _, channel := range channels {
			closeChannel(channel)
		}
		log.Println("channels closed...")
		close(dispatchChannel)

		wg.Wait()
		messageInfo := <-messageSizeChannel
		close(messageSizeChannel)

		fmt.Fprintf(os.Stderr, "Data sent: %d byte(s)\n", messageInfo.MessageSize)
		fmt.Fprintf(os.Stderr, "Data limit exceeded: %t\n", messageInfo.MessageSize > messageSizeLimit)
		fmt.Fprintf(os.Stderr, "# of messages sent: %d\n", messageInfo.MessageCount)
		fmt.Fprintf(os.Stderr, "Limit of messages exceeded: %t\n", messageInfo.MessageCount > messageCountLimit)
	}
	os.Exit(rc)
}
