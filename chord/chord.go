package chord

import "math"




type ChordNode struct{

	
	id int
	predecessor * ChordNode
	successor * ChordNode
	
	//key = index in finger table and  value = chord ID 
	fingerTable []int

	
}


func (chordNode * ChordNode) InitializeNode(ringSize int){
	fingerTableSize  := math.Log2(float64(ringSize))  
	chordNode.fingerTable = make([]int,int(fingerTableSize))


	//initialize predecessor
	chordNode.predecessor = nil
	chordNode.successor = nil
}
