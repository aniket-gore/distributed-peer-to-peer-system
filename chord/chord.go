package chord

import( 
	"621_proj/rpcclient"
	"strconv"
	"fmt"
)

type ServerInfo struct{
	ServerID  string   `json:"serverID"`
	Protocol  string   `json:"protocol"`
	IpAddress string   `json: "ipAddress"`
	Port      int      `json: "port"`
	//ChordID   int      `json: "chordID"`
}



type ChordNode struct{
	
	//exposed
	MyServerInfo ServerInfo
	MValue int
	FirstNode int
	//unexposed
	id uint32
	predecessor uint32
	successor uint32
	
	//key = index in finger table and  value = chord ID 
	fingerTable []uint32
	//map another chord ID to their actual server info
	ftServerMapping map[uint32]ServerInfo 
	
}



func (chordNode * ChordNode) InitializeNode(){
	
	//FT[i] = succ(id + 2^(i-1))   for 1<=i<=m
	chordNode.fingerTable = make([]uint32,int(chordNode.MValue) + 1)
	
	//initialize predecessor and successor to own ID
	
	chordNode.predecessor = chordNode.id
	chordNode.successor = chordNode.id
	
	if chordNode.FirstNode != 1{
		chordNode.join(getDefaultServerInfo())
	}else{
		chordNode.successor = chordNode.id
	}
}


func getDefaultServerInfo() ServerInfo{
	serverInfo := ServerInfo{}
	serverInfo.ServerID = "mainserver"
	serverInfo.Protocol = "tcp"
	serverInfo.IpAddress = "127.0.0.1"
	serverInfo.Port = 1234
	return serverInfo
}


func (chordNode * ChordNode) join(serverInfo ServerInfo){
	
	//create the connection
	//number of simultaneous channels should be taken from config file
	numChannels := 10
	network := serverInfo.Protocol
	address := serverInfo.IpAddress + ":" + strconv.Itoa(serverInfo.Port)
	serverName := serverInfo.ServerID
	client := &rpcclient.RPCClient{}
	
	//create new client
	if err :=client.NewClient(network,address,numChannels); err!=nil{
		fmt.Println(err)
		return
		
	} 
	jsonMessages := make([]string,0,10)
	
	jsonMessages = append(jsonMessages, "{\"method\":\"findSuccessor\",\"params\":[22]}")
	fmt.Println(jsonMessages[0])
	
	numMessages :=len(jsonMessages)
	//make asychronous calls
	if err := client.CreateAsyncRPC(jsonMessages[0:], serverName); err!=nil{
		fmt.Println(err)
		return
	}
	
	//process replies from server
	if err := client.ProcessReplies(numMessages); err!=nil{
		fmt.Println(err)
		return
	}

}
/*
// ask node n to find the successor of id
n.find successor(id)
if (id ∈ (n, successor])
return successor;
else
n = closest preceding node(id );
return n .find successor(id);


// search the local table for the highest predecessor of id
n.closest preceding node(id)
for i = m downto 1
if (finger[i] ∈ (n, id))
return finger[i];
return n;
*/

/*
func (chordNode  ChordNode) findSuccessor(inputId int){
	if inputId<=chordNode.id {
		return id
	}
	else{
		searchedId = closestPrecedingNode(inputId)
	}

	//rpc call to searchedId
	
}

func (chordNode  ChordNode) closestPrecedingNode(inputID int){
	for i:=chordNode.ringSize;i>=0;i--{
		if chordNode.fingerTable[i]< 
	}
}
*/
