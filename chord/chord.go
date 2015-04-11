package chord

import (
	"621_proj/rpcclient"
	"fmt"
	"hash/fnv"
	"math"
	"time"
	"log"
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
	
	Id          uint32
	Predecessor uint32
	Successor   uint32
	
	isPredecessorNil bool

	//key = index in finger table and  value = chord ID
	fingerTable []uint32
	//map another chord ID to their actual server info
	FtServerMapping map[uint32]ServerInfo

	//logger from rpcServer
	Logger       *log.Logger

}

func (chordNode *ChordNode) GetPredecessor() (bool, uint32) {
	if chordNode.isPredecessorNil {
		return true, 0
	} else {
		return false, chordNode.Predecessor
	}
}

func (chordNode *ChordNode) InitializeNode() {
	chordNode.Logger.Println("Chord : In Initialize Node")
	//FT[i] = succ(id + 2^(i-1))   for 1<=i<=m
	chordNode.fingerTable = make([]uint32, int(chordNode.MValue)+1)

	//initialize predecessor and successor to own ID
	chordNode.Id = getID(chordNode.MyServerInfo.IpAddress, chordNode.MyServerInfo.Port)

	chordNode.isPredecessorNil = true

	if chordNode.FirstNode != 1 {
		chordNode.join(getDefaultServerInfo())
	} else {
		chordNode.Logger.Println("Chord InitializeNode : Assigned Successor : " + fmt.Sprint(chordNode.Id))
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
	chordNode.Logger.Println(jsonMessage)

	clientServerInfo := rpcclient.ServerInfo{}
	clientServerInfo.ServerID = serverInfo.ServerID
	clientServerInfo.Protocol = serverInfo.Protocol
	clientServerInfo.IpAddress = serverInfo.IpAddress
	clientServerInfo.Port = serverInfo.Port

	client := &rpcclient.RPCClient{}
	err, response := client.RpcCall(clientServerInfo, jsonMessage)

	if err != nil {
		chordNode.Logger.Println(err)
		return
	}

	//test
	chordNode.Logger.Println(response)
	//test
	chordNode.Predecessor = 0
	chordNode.Successor =uint32((response.Result[0]).(float64))

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
func (chordNode *ChordNode) fixFingers(fingerTableIndex int) {

	chordNode.Logger.Println("Chord : In fixFingers")
	
	//find the successor of (p+2^(i-1)) by initiaing the find_successor call from the current node
	nextNodeId := chordNode.Id + uint32(math.Pow(2, float64(fingerTableIndex-1)))
	jsonMessage := "{\"method\":\"findSuccessor\",\"params\":[" + fmt.Sprint(nextNodeId) + "]}"

	chordNode.Logger.Println("Chord : In fixFingers :  nextNodeID node :" + fmt.Sprint(nextNodeId))

	clientServerInfo := rpcclient.ServerInfo{}
	clientServerInfo.ServerID = chordNode.MyServerInfo.ServerID
	clientServerInfo.Protocol = chordNode.MyServerInfo.Protocol
	clientServerInfo.IpAddress = chordNode.MyServerInfo.IpAddress
	clientServerInfo.Port = chordNode.MyServerInfo.Port

	client := &rpcclient.RPCClient{}
	err, response := client.RpcCall(clientServerInfo, jsonMessage)

	if err != nil {
		chordNode.Logger.Println(err)
		return
	}

	chordNode.fingerTable[fingerTableIndex] = uint32((response.Result[0]).(float64))
}

func (chordNode *ChordNode) stabilize() {
	chordNode.Logger.Println("Chord : In Stabilize")
	

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
		chordNode.Logger.Println(err)
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
		chordNode.Logger.Println(err)
		return
	}
}

func (chordNode *ChordNode) RunBackgroundProcesses() {
	chordNode.Logger.Println("Chord : In Run Background Processes Node")
	fingerTableIndex :=0
	ticker := time.NewTicker(time.Millisecond * 500)
	go func() {
		for t := range ticker.C {
			chordNode.Logger.Println("Tick at", t)
			chordNode.stabilize()
			
			//check every entry in the finger table one after another
			fingerTableIndex += 1
			if fingerTableIndex > chordNode.MValue {
				fingerTableIndex = 1
			}

			chordNode.fixFingers(fingerTableIndex)
	
		}
	}()
}
