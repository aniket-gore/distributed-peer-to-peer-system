package chord

import (
	"621_proj/rpcclient"
	"621_proj/hashing"
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"time"
	"encoding/json"
	"errors"
)

/*
type ServerInfo struct {
	ServerID  string `json:"serverID"`
	Protocol  string `json:"protocol"`
	IpAddress string `json: "ipAddress"`
	Port      int    `json: "port"`
}
*/

type ServerInfoWithID struct{
	ServerInfo rpcclient.ServerInfo `json:"serverInfo"`
	Id uint32 `json:"Id,string"`
}

type ChordNode struct {

	//exposed
	MyServerInfo rpcclient.ServerInfo
	MValue       int
	FirstNode    int

	Id          uint32
	Predecessor uint32
	Successor   uint32

	isPredecessorNil bool

	//key = index in finger table and  value = chord ID
	FingerTable []uint32
	//map another chord ID to their actual server info
	FtServerMapping map[uint32]rpcclient.ServerInfo

	//logger from rpcServer
	Logger *log.Logger
	KeyHashLength int
	RelationHashLength int
}

func (chordNode *ChordNode) updateFtServerMapping(id uint32, serverInfo rpcclient.ServerInfo) {

	if _, ok := chordNode.FtServerMapping[id]; !ok {

		clientServerInfo := rpcclient.ServerInfo{}
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

func (chordNode *ChordNode) SetPredecessor(isPredecessorNil bool, predecessor uint32) {
	chordNode.isPredecessorNil = isPredecessorNil
	chordNode.Predecessor = predecessor
}

func (chordNode *ChordNode) InitializeNode() {
	chordNode.Logger.Println("Chord : In Initialize Node")
	//FT[i] = succ(id + 2^(i-1))   for 1<=i<=m
	chordNode.FingerTable = make([]uint32, int(chordNode.MValue)+1)

	//create an empty map for server mappings
	chordNode.FtServerMapping = make(map[uint32]rpcclient.ServerInfo)

	//initialize predecessor and successor to own ID
	chordNode.Id = getID(chordNode.MyServerInfo.IpAddress, chordNode.MyServerInfo.Port)
	
	//chordNode.Id = getSHAID(chordNode.MyServerInfo.IpAddress, chordNode.MyServerInfo.Port, chordNode.MValue)
	
	
	chordNode.isPredecessorNil = true

	if chordNode.FirstNode != 1 {

		chordNode.join(getDefaultServerInfo())

	} else {

		chordNode.Logger.Println("Chord InitializeNode : Assigned Successor : " + fmt.Sprint(chordNode.Id))
		chordNode.Successor = chordNode.Id
	}
}

func getDefaultServerInfo() rpcclient.ServerInfo {
	serverInfo := rpcclient.ServerInfo{}
	serverInfo.ServerID = "mainserver"
	serverInfo.Protocol = "tcp"
	serverInfo.IpAddress = "127.0.0.1"
	serverInfo.Port = 1234

	return serverInfo
}

func (chordNode *ChordNode) join(serverInfo rpcclient.ServerInfo) {

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
	chordNode.Successor = uint32((response.(*(rpcclient.ResponseParameters)).Result[0]).(float64))

	resultServerInfo := rpcclient.ServerInfo{}
	for key, value := range response.(*(rpcclient.ResponseParameters)).Result[1].(map[string]interface{}) {
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

func getSHAID(ipAddress string, port int, mBits int) uint32{
	return hashing.GetStartingBits(ipAddress + "_" + string(port),mBits)
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
		if chordNode.FingerTable[i] > chordNode.Id && chordNode.FingerTable[i] < inputId && chordNode.FingerTable[i] != 0 {
			return chordNode.FingerTable[i]
		}
	}

	return chordNode.Id
}

// initially FingerTableIndex = 0
func (chordNode *ChordNode) fixFingers(FingerTableIndex int) {

	chordNode.Logger.Println("Chord : In fixFingers")

	//find the successor of (p+2^(i-1)) by initiaing the find_successor call from the current node
	nextNodeId := chordNode.Id + uint32(math.Pow(2, float64(FingerTableIndex-1)))
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

	// process only if response is present
	if response.(*(rpcclient.ResponseParameters)).Result != nil {
		chordNode.FingerTable[FingerTableIndex] = uint32((response.(*(rpcclient.ResponseParameters)).Result[0]).(float64))

		resultServerInfo := rpcclient.ServerInfo{}
		for key, value := range response.(*(rpcclient.ResponseParameters)).Result[1].(map[string]interface{}) {
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
		chordNode.FtServerMapping[chordNode.FingerTable[FingerTableIndex]] = resultServerInfo
		chordNode.Successor = chordNode.FingerTable[1]
	}
	chordNode.Logger.Println("FingerTable=", chordNode.FingerTable)
}

func (chordNode *ChordNode) stabilize() {
	chordNode.Logger.Println("Chord : In Stabilize")

	//RPC call to get predecessor of successor
	jsonMessage := "{\"method\":\"GetPredecessor\",\"params\":[]}"

	clientServerInfo := rpcclient.ServerInfo{}
	clientServerInfo.ServerID = chordNode.FtServerMapping[chordNode.FingerTable[1]].ServerID
	clientServerInfo.Protocol = chordNode.FtServerMapping[chordNode.FingerTable[1]].Protocol
	clientServerInfo.IpAddress = chordNode.FtServerMapping[chordNode.FingerTable[1]].IpAddress
	clientServerInfo.Port = chordNode.FtServerMapping[chordNode.FingerTable[1]].Port

	client := &rpcclient.RPCClient{}
	err, response := client.RpcCall(clientServerInfo, jsonMessage)

	if err != nil {
		chordNode.Logger.Println(err)
		return
	}

	// process only if response is present -- CASE WHERE SUCCESSOR LEAVES ABRUPTLY
	if response.(*(rpcclient.ResponseParameters)).Result != nil {
		isPredecessorOfSuccessorNil := (response.(*(rpcclient.ResponseParameters)).Result[0]).(bool)
		predecessorOfSuccessor := uint32((response.(*(rpcclient.ResponseParameters)).Result[1]).(float64))

		resultServerInfo := rpcclient.ServerInfo{}
		for key, value := range response.(*(rpcclient.ResponseParameters)).Result[2].(map[string]interface{}) {
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
			if (predecessorOfSuccessor > chordNode.Id && predecessorOfSuccessor < chordNode.Successor) ||
				(!isPredecessorNil && chordNode.Successor == chordNode.Id) ||
				(chordNode.Successor < chordNode.Id && predecessorOfSuccessor < chordNode.Successor) ||
				(chordNode.Successor < chordNode.Id && predecessorOfSuccessor > chordNode.Successor && predecessorOfSuccessor > chordNode.Id) {
				chordNode.Successor = predecessorOfSuccessor
				chordNode.FtServerMapping[chordNode.Successor] = predecessorOfSuccessorServerInfo
			}
		}
	}

	chordNode.Logger.Println("About to make RPC call: Notify")
	//RPC call to notify the successor about the predecessor(i.e. current node)
	jsonMessage = "{\"method\":\"Notify\",\"params\":[" + fmt.Sprint(chordNode.Id) + ", {\"serverID\":\"" + chordNode.MyServerInfo.ServerID + "\", \"protocol\":\"" + chordNode.MyServerInfo.Protocol + "\",\"IpAddress\":\"" + chordNode.MyServerInfo.IpAddress + "\",\"Port\":" + fmt.Sprint(chordNode.MyServerInfo.Port) + "}]}"

	clientServerInfo = rpcclient.ServerInfo{}
	clientServerInfo.ServerID = chordNode.FtServerMapping[chordNode.FingerTable[1]].ServerID
	clientServerInfo.Protocol = chordNode.FtServerMapping[chordNode.FingerTable[1]].Protocol
	clientServerInfo.IpAddress = chordNode.FtServerMapping[chordNode.FingerTable[1]].IpAddress
	clientServerInfo.Port = chordNode.FtServerMapping[chordNode.FingerTable[1]].Port

	client = &rpcclient.RPCClient{}
	err, _ = client.RpcCall(clientServerInfo, jsonMessage)

	if err != nil {
		chordNode.Logger.Println(err)
		return
	}
}


func (chordNode *ChordNode) checkPredecessor() {

	chordNode.Logger.Println("Chord : In checkPredecessor")

	//RPC call to check predecessor
	jsonMessage := "{\"method\":\"CheckPredecessor\",\"params\":[]}"

	chordNode.Logger.Println("Predecessor:", chordNode.Predecessor)
	chordNode.Logger.Println("Predecessor serverInfo:", chordNode.FtServerMapping[chordNode.Predecessor])
	clientServerInfo := rpcclient.ServerInfo{}
	clientServerInfo.ServerID = chordNode.FtServerMapping[chordNode.Predecessor].ServerID
	clientServerInfo.Protocol = chordNode.FtServerMapping[chordNode.Predecessor].Protocol
	clientServerInfo.IpAddress = chordNode.FtServerMapping[chordNode.Predecessor].IpAddress
	clientServerInfo.Port = chordNode.FtServerMapping[chordNode.Predecessor].Port

	chordNode.Logger.Println("RPC call to:", clientServerInfo)

	client := &rpcclient.RPCClient{}
	err, response := client.RpcCall(clientServerInfo, jsonMessage)

	chordNode.Logger.Println("Response:", response)

	if err != nil {
		chordNode.Logger.Println(err)
		return
	}

	chordNode.Logger.Println("response.Result=", response.(*(rpcclient.ResponseParameters)).Result)
	// Set the predecessor to nil if empty response
	if response.(*(rpcclient.ResponseParameters)).Result == nil {
		chordNode.Logger.Println("SETTING PREDECESSOR TO NIL")
		chordNode.SetPredecessor(true, 0)
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
			for FingerTableIndex := 1; FingerTableIndex <= chordNode.MValue; FingerTableIndex++ {
				chordNode.fixFingers(FingerTableIndex)
			}

			//check if predecessor is nil
			chordNode.checkPredecessor()
		}
	}()
}

//transfer keys to successor 
//n may notify its predecessor p and successor s before leaving
func (chordNode *ChordNode) NotifyShutDownToRing(){
	
	
	if !chordNode.isPredecessorNil{
		//rpc call to successor - tell successor about my predecessor
		//create paramter to be sent - predecessor
		parameter := ServerInfoWithID{}
		parameter.Id = chordNode.Predecessor
		parameter.ServerInfo = chordNode.FtServerMapping[chordNode.Predecessor]
		
		//create json message
		jsonMessage := rpcclient.RequestParameters{}
		jsonMessage.Method = "PredecessorLeft";
		jsonMessage.Params = make([]interface{},1)
		jsonMessage.Params[0] = parameter
		jsonBytes,err :=json.Marshal(jsonMessage)
		if err!=nil{
			chordNode.Logger.Println(err)
			return
		} 
               
		chordNode.Logger.Println(string(jsonBytes))
	
		//prepare server info
		clientServerInfo,err := chordNode.PrepareClientServerInfo(chordNode.FingerTable[1])
		if err==nil{
		
			client := &rpcclient.RPCClient{}
			err, _ := client.RpcCall(clientServerInfo, string(jsonBytes))

			if err != nil {
				chordNode.Logger.Println(err)
			}

		}else{
			chordNode.Logger.Println(err)
		}
		
		
		//rpc call to predecessor - tell predecessor about my successor
		//create paramter - successor
		parameter = ServerInfoWithID{}
		parameter.Id = chordNode.FingerTable[1]
		parameter.ServerInfo = chordNode.FtServerMapping[chordNode.FingerTable[1]]
	
		//create json message
		jsonMessage = rpcclient.RequestParameters{}
		jsonMessage.Method = "SuccessorLeft";
		jsonMessage.Params = make([]interface{},1)
		jsonMessage.Params[0] = parameter
		jsonBytes,err =json.Marshal(jsonMessage)
		if err!=nil{
			chordNode.Logger.Println(err)
			return
		} 
               
		chordNode.Logger.Println(string(jsonBytes))
	
		clientServerInfo,err = chordNode.PrepareClientServerInfo(chordNode.Predecessor)
		if err==nil{
			
			client := &rpcclient.RPCClient{}
			err, _ := client.RpcCall(clientServerInfo, string(jsonBytes))
			
			if err != nil {
				chordNode.Logger.Println(err)
			}
			
		}else{
			chordNode.Logger.Println(err)
		}
	
	}//if not predecessor nil
	

	

	
}

/*
Use this function to create server info to whom RPCcall is going to be made
Input: Chord Id of the server
*/
func (chordNode *ChordNode)PrepareClientServerInfo(chordID uint32)(rpcclient.ServerInfo,error){
	clientServerInfo := rpcclient.ServerInfo{}
	if _,ok := chordNode.FtServerMapping[chordID]; ok {
		clientServerInfo.ServerID = chordNode.FtServerMapping[chordID].ServerID
		clientServerInfo.Protocol = chordNode.FtServerMapping[chordID].Protocol
		clientServerInfo.IpAddress = chordNode.FtServerMapping[chordID].IpAddress
		clientServerInfo.Port = chordNode.FtServerMapping[chordID].Port
		return clientServerInfo,nil
	}else{
		customError := errors.New("Entry not found in FTServerMapping")
		chordNode.Logger.Println(customError)
		return rpcclient.ServerInfo{},customError
	}
	
}


/*
get node for forwarding the request 
*/

func (chordNode * ChordNode)ForwardInsert(reqPar []interface{}) (ServerInfoWithID,error) {
	//Unmarshal into array of interfaces
	var parameters []interface{}
	parameters = reqPar

	//Use dict3 struct to unmarshall
	var key string
	var relation string
	for k, v := range parameters {
	
		if k == 0 {
			key = v.(string)
		} else if k == 1 {
			relation = v.(string)
		} else if k == 2 {
			//dict3.Value = v
		}
	}


	//get hash values for key and relation
	keyHash :=hashing.GetStartingBits(key,chordNode.KeyHashLength)
	relationHash := hashing.GetStartingBits(relation,chordNode.RelationHashLength)
	
	var finalChordID uint32
	finalChordID = keyHash<<uint(chordNode.RelationHashLength) | relationHash
	
	//findsuccessor for finalChordID
			
	//create json message
	jsonMessage := rpcclient.RequestParameters{}
	jsonMessage.Method = "findSuccessor";
	jsonMessage.Params = make([]interface{},1)
	jsonMessage.Params[0] = finalChordID
	jsonBytes,err :=json.Marshal(jsonMessage)
	if err!=nil{
		chordNode.Logger.Println(err)
		return ServerInfoWithID{},err
	} 
               
	chordNode.Logger.Println(string(jsonBytes))
	
	//prepare server info
	clientServerInfo,err := chordNode.PrepareClientServerInfo(chordNode.FingerTable[1])
	if err!=nil{
		chordNode.Logger.Println(err)
		return ServerInfoWithID{},err
	}
	client := &rpcclient.RPCClient{}
	err, response := client.RpcCall(clientServerInfo, string(jsonBytes))
		
	if err != nil {
		chordNode.Logger.Println(err)
		return ServerInfoWithID{},err
	}
		
	var successorInfo ServerInfoWithID
	successorInfo.Id = uint32((response.(*(rpcclient.ResponseParameters)).Result[0]).(float64))
	
	//again marshal and unmarshal - reason it was getting marshalled into map[string]interface{}
	serverInfoBytes,_ := json.Marshal(response.(*(rpcclient.ResponseParameters)).Result[1])
	if err = json.Unmarshal(serverInfoBytes,&(successorInfo.ServerInfo)); err!=nil{
		chordNode.Logger.Println(err)
		return ServerInfoWithID{},err
	}

	
	

	return successorInfo,nil
}
