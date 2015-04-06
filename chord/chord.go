package chord

import (
	"621_proj/rpcclient"
	"fmt"
	"hash/fnv"
)

type ServerInfo struct {
	ServerID  string `json:"serverID"`
	Protocol  string `json:"protocol"`
	IpAddress string `json: "ipAddress"`
	Port      int    `json: "port"`
	//ChordID   int      `json: "chordID"`
}

type ChordNode struct {

	//exposed
	MyServerInfo ServerInfo
	MValue       int
	FirstNode    int
	//unexposed
	id          uint32
	predecessor uint32
	successor   uint32

	//key = index in finger table and  value = chord ID
	fingerTable []uint32
	//map another chord ID to their actual server info
	ftServerMapping map[uint32]ServerInfo
}

func (chordNode *ChordNode) InitializeNode() {

	//FT[i] = succ(id + 2^(i-1))   for 1<=i<=m
	chordNode.fingerTable = make([]uint32, int(chordNode.MValue)+1)

	//initialize predecessor and successor to own ID
	chordNode.id = getID(chordNode.MyServerInfo.IpAddress, chordNode.MyServerInfo.Port)

	chordNode.predecessor = chordNode.id
	chordNode.successor = chordNode.id

	if chordNode.FirstNode != 1 {
		chordNode.join(getDefaultServerInfo())
	} else {
		chordNode.successor = chordNode.id
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

	jsonMessage := "{\"method\":\"findSuccessor\",\"params\":[" + fmt.Sprint(chordNode.id) + "]}"
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

}

func getID(ipAddress string, port int) uint32 {
	return calculateHash(ipAddress + "_" + string(port))
}

func calculateHash(stringValue string) uint32 {
	hasher := fnv.New32()
	hasher.Write([]byte(stringValue))
	return hasher.Sum32()
}
