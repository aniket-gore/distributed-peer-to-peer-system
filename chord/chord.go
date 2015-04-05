package chord

import (
	"hash/fnv"
	"math"
)

type ChordNode struct {
	id          uint32
	predecessor *ChordNode
	successor   *ChordNode

	//key = index in finger table and  value = chord ID
	fingerTable []int
}

func (chordNode *ChordNode) InitializeNode(ringSize int, ipAddress string, port int) {
	fingerTableSize := math.Log2(float64(ringSize))
	chordNode.fingerTable = make([]int, int(fingerTableSize))

	//get a unique identifier for the Chord node
	chordNode.id = getID(ipAddress, port)

	//initialize predecessor
	chordNode.predecessor = nil
	chordNode.successor = nil
}

func getID(ipAddress string, port int) uint32 {
	return calculateHash(ipAddress + "_" + string(port))
}

func calculateHash(stringValue string) uint32 {
	hasher := fnv.New32()
	hasher.Write([]byte(stringValue))
	return hasher.Sum32()
}
