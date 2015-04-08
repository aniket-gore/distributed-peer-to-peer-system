package chord

import (
	"621_proj/rpcclient"
	"fmt"
	"hash/fnv"
	"math"
)

type ServerInfo struct {
	ServerID  string `json:"serverID"`
	Protocol  string `json:"protocol"`
	IpAddress string `json: "ipAddress"`
	Port      int    `json: "port"`
}

type ChordNode struct {

	//exposed
	MyServerInfo ServerInfo
	MValue       int
	FirstNode    int
	//unexposed
	Id          uint32
	Predecessor uint32
	Successor   uint32

	isPredecessorNil bool

	//key = index in finger table and  value = chord ID
	fingerTable []uint32
	//map another chord ID to their actual server info
	FtServerMapping map[uint32]ServerInfo
}

func (chordNode *ChordNode) GetPredecessor() (bool, uint32) {
	if chordNode.isPredecessorNil {
		return true, 0
	} else {
		return false, chordNode.Predecessor
	}
}

func (chordNode *ChordNode) InitializeNode() {

	//FT[i] = succ(id + 2^(i-1))   for 1<=i<=m
	chordNode.fingerTable = make([]uint32, int(chordNode.MValue)+1)

	//initialize predecessor and successor to own ID
	chordNode.Id = getID(chordNode.MyServerInfo.IpAddress, chordNode.MyServerInfo.Port)

	chordNode.isPredecessorNil = true

	if chordNode.FirstNode != 1 {
		chordNode.join(getDefaultServerInfo())
	} else {
		chordNode.Successor = chordNode.Id
	}
}

func getDefaultServerInfo() ServerInfo {
	serverInfo := ServerInfo{}
	serverInfo.ServerID = "mainserver"
	serverInfo.Protocol = "tcp"
	serverInfo.IpAddress = "127.0.0.1"
	serverInfo.Port = 1234
	return serverInfo
}

func (chordNode *ChordNode) join(serverInfo ServerInfo) {

	jsonMessage := "{\"method\":\"findSuccessor\",\"params\":[" + fmt.Sprint(chordNode.Id) + "]}"
	fmt.Println(jsonMessage)

	clientServerInfo := rpcclient.ServerInfo{}
	clientServerInfo.ServerID = serverInfo.ServerID
	clientServerInfo.Protocol = serverInfo.Protocol
	clientServerInfo.IpAddress = serverInfo.IpAddress
	clientServerInfo.Port = serverInfo.Port

	client := &rpcclient.RPCClient{}
	err, response := client.RpcCall(clientServerInfo, jsonMessage)

	if err != nil {
		fmt.Println(err)
		return
	}

	//test
	fmt.Println(response)
	//test
	chordNode.Predecessor = 0
	chordNode.Successor = (response.Result[0]).(uint32)

}

func getID(ipAddress string, port int) uint32 {
	return calculateHash(ipAddress + "_" + string(port))
}

func calculateHash(stringValue string) uint32 {
	hasher := fnv.New32()
	hasher.Write([]byte(stringValue))
	return hasher.Sum32()
}

func (chordNode ChordNode) ClosestPrecedingNode(inputId uint32) uint32 {

	for i := chordNode.MValue; i > 0; i-- {
		//finger[i] ∈ (n, id)
		if chordNode.fingerTable[i] > chordNode.Id && chordNode.fingerTable[i] < inputId && chordNode.fingerTable[i] != 0 {
			return chordNode.fingerTable[i]
		}
	}

	return 0
}

// initially fingerTableIndex = 0
func (chordNode *ChordNode) FixFingers(serverInfo ServerInfo, fingerTableIndex int) {
	//check every entry in the finger table one after another
	fingerTableIndex += 1
	if fingerTableIndex > chordNode.MValue {
		fingerTableIndex = 1
	}

	//find the successor of (p+2^(i-1)) by initiaing the find_successor call from the current node
	nextNodeId := chordNode.Id + uint32(math.Pow(2, float64(fingerTableIndex-1)))
	jsonMessage := "{\"method\":\"findSuccessor\",\"params\":[" + fmt.Sprint(nextNodeId) + "]}"

	clientServerInfo := rpcclient.ServerInfo{}
	clientServerInfo.ServerID = chordNode.MyServerInfo.ServerID
	clientServerInfo.Protocol = chordNode.MyServerInfo.Protocol
	clientServerInfo.IpAddress = chordNode.MyServerInfo.IpAddress
	clientServerInfo.Port = chordNode.MyServerInfo.Port

	client := &rpcclient.RPCClient{}
	err, response := client.RpcCall(clientServerInfo, jsonMessage)

	if err != nil {
		fmt.Println(err)
		return
	}

	chordNode.fingerTable[fingerTableIndex] = (response.Result[0]).(uint32)
}

func (chordNode *ChordNode) Stabilize() {
	//RPC call to get predecessor of successor
	jsonMessage := "{\"method\":\"GetPredecessor\",\"params\":[]}"

	clientServerInfo := rpcclient.ServerInfo{}
	clientServerInfo.ServerID = chordNode.FtServerMapping[chordNode.fingerTable[0]].ServerID
	clientServerInfo.Protocol = chordNode.FtServerMapping[chordNode.fingerTable[0]].Protocol
	clientServerInfo.IpAddress = chordNode.FtServerMapping[chordNode.fingerTable[0]].IpAddress
	clientServerInfo.Port = chordNode.FtServerMapping[chordNode.fingerTable[0]].Port

	client := &rpcclient.RPCClient{}
	err, response := client.RpcCall(clientServerInfo, jsonMessage)

	if err != nil {
		fmt.Println(err)
		return
	}

	isPredecessorOfSuccessorNil := (response.Result[0]).(bool)
	predecessorOfSuccessor := (response.Result[1]).(uint32)

	//update the successor
	if !isPredecessorOfSuccessorNil {
		isPredecessorNil, predecessor := chordNode.GetPredecessor()
		//predecessor == chordNode.Id refers to the case where the ActualNodesInRing = 1 i.e. predecessor is the node itself
		if (predecessorOfSuccessor > chordNode.Id && predecessorOfSuccessor < chordNode.Successor) || (!isPredecessorNil && predecessor == chordNode.Id) {
			chordNode.Successor = predecessorOfSuccessor
		}
	}

	//RPC call to notify the successor about the predecessor(i.e. current node)
	jsonMessage = "{\"method\":\"Notify\",\"params\":[" + fmt.Sprint(chordNode.Id) + "]}"

	clientServerInfo = rpcclient.ServerInfo{}
	clientServerInfo.ServerID = chordNode.FtServerMapping[chordNode.fingerTable[0]].ServerID
	clientServerInfo.Protocol = chordNode.FtServerMapping[chordNode.fingerTable[0]].Protocol
	clientServerInfo.IpAddress = chordNode.FtServerMapping[chordNode.fingerTable[0]].IpAddress
	clientServerInfo.Port = chordNode.FtServerMapping[chordNode.fingerTable[0]].Port

	client = &rpcclient.RPCClient{}
	err, _ = client.RpcCall(clientServerInfo, jsonMessage)

	if err != nil {
		fmt.Println(err)
		return
	}
}
