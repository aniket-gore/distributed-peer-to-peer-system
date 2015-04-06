package chord

import (
	"621_proj/rpcclient"
	"fmt"
	"hash/fnv"
)

type ServerInfo struct{
	ServerID  string   `json:"serverID"`
	Protocol  string   `json:"protocol"`
	IpAddress string   `json: "ipAddress"`
	Port      int      `json: "port"`
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

	//key = index in finger table and  value = chord ID
	fingerTable []uint32
	//map another chord ID to their actual server info
	FtServerMapping map[uint32]ServerInfo
}

func (chordNode *ChordNode) InitializeNode() {

	//FT[i] = succ(id + 2^(i-1))   for 1<=i<=m
	chordNode.fingerTable = make([]uint32, int(chordNode.MValue)+1)

	//initialize predecessor and successor to own ID
	chordNode.Id = getID(chordNode.MyServerInfo.IpAddress, chordNode.MyServerInfo.Port)

	chordNode.Predecessor = chordNode.Id
	chordNode.Successor = chordNode.Id

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

/*
Wrongly done - Will be fixed by aniket
no mod used
also checking from down is not right
*/

func (chordNode ChordNode)ClosestPrecedingNode(inputId uint32) uint32 {
	
	for i:=chordNode.MValue;i>0;i--{
		//finger[i] ∈ (n, id)
		if chordNode.fingerTable[i]>chordNode.Id && chordNode.fingerTable[i]<inputId  && chordNode.fingerTable[i]!=0{
			return chordNode.fingerTable[i]
		}
	}
	
	return 0
}
