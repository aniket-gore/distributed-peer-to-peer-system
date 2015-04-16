package chord

import (
	"621_proj/rpcclient"
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"time"
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
	Logger *log.Logger
}

func (chordNode *ChordNode) updateFtServerMapping(id uint32, serverInfo ServerInfo) {

	if _, ok := chordNode.FtServerMapping[id]; !ok {

		clientServerInfo := ServerInfo{}
		clientServerInfo.ServerID = serverInfo.ServerID
		clientServerInfo.Protocol = serverInfo.Protocol
		clientServerInfo.IpAddress = serverInfo.IpAddress
		clientServerInfo.Port = serverInfo.Port

		chordNode.FtServerMapping[id] = clientServerInfo
	}
}

func (chordNode *ChordNode) GetPredecessor() (bool, uint32) {
	if chordNode.isPredecessorNil {
		return true, 0
	} else {
		return false, chordNode.Predecessor
	}
}

func (chordNode *ChordNode) SetPredecessor(predecessor uint32) {
	chordNode.isPredecessorNil = false
	chordNode.Predecessor = predecessor
}

func (chordNode *ChordNode) InitializeNode() {
	chordNode.Logger.Println("Chord : In Initialize Node")
	//FT[i] = succ(id + 2^(i-1))   for 1<=i<=m
	chordNode.fingerTable = make([]uint32, int(chordNode.MValue)+1)

	//create an empty map for server mappings
	chordNode.FtServerMapping = make(map[uint32]ServerInfo)

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

	chordNode.Logger.Println("Chord : In Join")
	jsonMessage := "{\"method\":\"findSuccessor\",\"params\":[" + fmt.Sprint(chordNode.Id) + "]}"

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

	chordNode.Predecessor = 0
	chordNode.Successor = uint32((response.Result[0]).(float64))

	resultServerInfo := ServerInfo{}
	for key, value := range response.Result[1].(map[string]interface{}) {
		switch key {
		case "serverID":
			resultServerInfo.ServerID = value.(string)
			break
		case "protocol":
			resultServerInfo.Protocol = value.(string)
			break
		case "IpAddress":
			resultServerInfo.IpAddress = value.(string)
			break
		case "Port":
			resultServerInfo.Port = int(value.(float64))
		}
	}
	chordNode.FtServerMapping[chordNode.Successor] = resultServerInfo
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

	chordNode.Logger.Println("Chord : In ClosestPrecedingNode")
	for i := chordNode.MValue; i > 0; i-- {
		//finger[i] ∈ (n, id)
		if chordNode.fingerTable[i] > chordNode.Id && chordNode.fingerTable[i] < inputId && chordNode.fingerTable[i] != 0 {
			return chordNode.fingerTable[i]
		}
	}

	return chordNode.Id
}

// initially fingerTableIndex = 0
func (chordNode *ChordNode) fixFingers(fingerTableIndex int) {

	chordNode.Logger.Println("Chord : In fixFingers")

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
		chordNode.Logger.Println(err)
		return
	}

	chordNode.fingerTable[fingerTableIndex] = uint32((response.Result[0]).(float64))

	resultServerInfo := ServerInfo{}
	for key, value := range response.Result[1].(map[string]interface{}) {
		switch key {
		case "serverID":
			resultServerInfo.ServerID = value.(string)
			break
		case "protocol":
			resultServerInfo.Protocol = value.(string)
			break
		case "IpAddress":
			resultServerInfo.IpAddress = value.(string)
			break
		case "Port":
			resultServerInfo.Port = int(value.(float64))
		}
	}
	chordNode.FtServerMapping[chordNode.fingerTable[fingerTableIndex]] = resultServerInfo
	chordNode.Successor = chordNode.fingerTable[1]

	chordNode.Logger.Println("FingerTable=", chordNode.fingerTable)
}

func (chordNode *ChordNode) stabilize() {
	chordNode.Logger.Println("Chord : In Stabilize")

	//RPC call to get predecessor of successor
	jsonMessage := "{\"method\":\"GetPredecessor\",\"params\":[]}"

	clientServerInfo := rpcclient.ServerInfo{}
	clientServerInfo.ServerID = chordNode.FtServerMapping[chordNode.fingerTable[1]].ServerID
	clientServerInfo.Protocol = chordNode.FtServerMapping[chordNode.fingerTable[1]].Protocol
	clientServerInfo.IpAddress = chordNode.FtServerMapping[chordNode.fingerTable[1]].IpAddress
	clientServerInfo.Port = chordNode.FtServerMapping[chordNode.fingerTable[1]].Port

	client := &rpcclient.RPCClient{}
	err, response := client.RpcCall(clientServerInfo, jsonMessage)

	if err != nil {
		chordNode.Logger.Println(err)
		return
	}

	isPredecessorOfSuccessorNil := (response.Result[0]).(bool)
	predecessorOfSuccessor := uint32((response.Result[1]).(float64))

	resultServerInfo := ServerInfo{}
	for key, value := range response.Result[2].(map[string]interface{}) {
		switch key {
		case "serverID":
			resultServerInfo.ServerID = value.(string)
			break
		case "protocol":
			resultServerInfo.Protocol = value.(string)
			break
		case "IpAddress":
			resultServerInfo.IpAddress = value.(string)
			break
		case "Port":
			resultServerInfo.Port = int(value.(float64))
		}
	}
	predecessorOfSuccessorServerInfo := resultServerInfo

	//update the successor
	if !isPredecessorOfSuccessorNil {
		isPredecessorNil, _ := chordNode.GetPredecessor()
		//predecessor == chordNode.Id refers to the case where the ActualNodesInRing = 1 i.e. predecessor is the node itself
		if (predecessorOfSuccessor > chordNode.Id && predecessorOfSuccessor < chordNode.Successor) || (!isPredecessorNil && chordNode.Successor == chordNode.Id) {
			chordNode.Successor = predecessorOfSuccessor
			chordNode.FtServerMapping[chordNode.Successor] = predecessorOfSuccessorServerInfo
		}
	}

	//RPC call to notify the successor about the predecessor(i.e. current node)
	jsonMessage = "{\"method\":\"Notify\",\"params\":[" + fmt.Sprint(chordNode.Id) + ", {\"serverID\":\"" + chordNode.MyServerInfo.ServerID + "\", \"protocol\":\"" + chordNode.MyServerInfo.Protocol + "\",\"IpAddress\":\"" + chordNode.MyServerInfo.IpAddress + "\",\"Port\":" + fmt.Sprint(chordNode.MyServerInfo.Port) + "}]}"

	clientServerInfo = rpcclient.ServerInfo{}
	clientServerInfo.ServerID = chordNode.FtServerMapping[chordNode.fingerTable[1]].ServerID
	clientServerInfo.Protocol = chordNode.FtServerMapping[chordNode.fingerTable[1]].Protocol
	clientServerInfo.IpAddress = chordNode.FtServerMapping[chordNode.fingerTable[1]].IpAddress
	clientServerInfo.Port = chordNode.FtServerMapping[chordNode.fingerTable[1]].Port

	client = &rpcclient.RPCClient{}
	err, _ = client.RpcCall(clientServerInfo, jsonMessage)

	if err != nil {
		chordNode.Logger.Println(err)
		return
	}
}

func (chordNode *ChordNode) RunBackgroundProcesses() {
	ticker := time.NewTicker(time.Millisecond * 2000)
	go func() {
		for t := range ticker.C {
			chordNode.Logger.Println("Tick at", t)
			chordNode.Logger.Println("Chord : In RunBackgroundProcesses")
			chordNode.Logger.Println("Successor=", chordNode.Successor)
			chordNode.Logger.Println("Predecessor=", chordNode.Predecessor)
			chordNode.stabilize()

			//check every entry in the finger table one after another
			for fingerTableIndex := 1; fingerTableIndex <= chordNode.MValue; fingerTableIndex++ {
				chordNode.fixFingers(fingerTableIndex)
			}
		}
	}()
}
