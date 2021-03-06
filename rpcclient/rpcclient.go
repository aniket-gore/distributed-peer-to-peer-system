package rpcclient

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/rpc"
	"net/rpc/jsonrpc"
	"strconv"
	"time"
)

type ServerInfo struct {
	ServerID  string `json:"serverID"`
	Protocol  string `json:"protocol"`
	IpAddress string `json: "ipAddress"`
	Port      int    `json: "port"`

}

//structure for parsing Response from server

type ResponseParameters struct {
	Result []interface{} `json:"result"`
	Id     int           `json: ",omitempty"`
	Error  interface{}   `json:"error"`
}

//structure for getting the request name
type RequestParameters struct {
	Method string `json:"method,omitempty"`
	//Params json.RawMessage `json: "params"`
	Params []interface{} `json: "params"`
	Id     int           `json:",omitempty"`
}

//to manage inserts
type ResponseParametersInsert struct {
	Result interface{} `json:"result"`
	Id     int         `json: "id,omitempty"`
	Error  interface{} `json:"error"`
}

//structure for parsing config file
type ConfigType struct {
	ServerID  string   `json:"serverID"`
	Protocol  string   `json:"protocol"`
	IpAddress string   `json: "ipAddress"`
	Port      int      `json: "port"`
	Methods   []string `json: "methods"`
}

//read config file
func (configObject *ConfigType) ReadConfig(configFilePath string) error {
	file, e := ioutil.ReadFile(configFilePath)
	if e != nil {
		fmt.Printf("File Error: %v\n", e)
		return e
	}

	if e = json.Unmarshal(file, configObject); e != nil {
		fmt.Printf("JSON Marshalling Error: %v\n", e)
		return e

	}

	return nil

}

//Structure for RPC Client
type RPCClient struct {
	//the client connection
	connection *rpc.Client
	//number of simultaneous channels
	numChannels int
	//a channel containing pointers to simultaneous RPC calls
	doneChan chan *rpc.Call

	//add timeout as well

}

//RPCClient creator
func (client *RPCClient) NewClient(network string, address string) error {
	numChannels := 10
	conn, err := jsonrpc.Dial(network, address)
	client.connection = conn
	if err != nil {
		fmt.Println(err)
		return err
	}
	//create a buffered channel for buffering simultaneous calls
	client.doneChan = make(chan *rpc.Call, numChannels)

	return nil
}

func extractMethodName(byteRequest []byte, rpcFunction *string) error {
	/*
		Method string `json:"Method"`
		Params json.RawMessage `json: "params"`
		Id int 'json" "id"'
	*/

	var reqPar RequestParameters
	if err := json.Unmarshal(byteRequest, &reqPar); err != nil {
		customError := errors.New("Message request unmarshalling error:" + err.Error())
		fmt.Println(customError)
		return customError

	}
	//check if it ins in the list of methd names
	firstLetter := reqPar.Method[0]
	if firstLetter >= 97 && firstLetter <= 122 {
		(*rpcFunction) = string(reqPar.Method[0]-'a'+65) + reqPar.Method[1:]
	} else {
		(*rpcFunction) = reqPar.Method
	}

	return nil

}

//create Asynchronous RPC calls
func (client *RPCClient) CreateAsyncRPC(jsonMessage string, serverName string) error {
	var byteRequest []byte

	//directly send jsonmessages to the server asynchronously
	var rpcFunction string

	byteRequest = []byte(jsonMessage)

	var reqPar RequestParameters
	/*
	   Method string `json:"Method"`
	   Params json.RawMessage `json: "params"`
	   Id int 'json" "id"'
	*/
	//fmt.Println("Request: ",request)
	if err := json.Unmarshal(byteRequest, &reqPar); err != nil {
		customError := errors.New("Message request unmarshalling error:" + err.Error())
		fmt.Println(customError)
		return customError

	}

	//fmt.Println("Request",reqPar)
	if err := extractMethodName(byteRequest, &rpcFunction); err != nil {
		fmt.Println(err)
		return err

	}
	rpcServerAndFunction := serverName + "." + rpcFunction

	//encoder := json.NewEncoder(os.Stdout)
	//encoder.Encode(reqPar)
	var response interface{}

	if rpcFunction == "Insert" {
		response = new(ResponseParametersInsert)
	} else {
		response = new(ResponseParameters)
	}
	client.connection.Go(rpcServerAndFunction, reqPar, response, client.doneChan)

	return nil

}

//process calls by reading the channel of Calls
func (client *RPCClient) ProcessReply() (error, interface{}) {

	defer client.connection.Close()
	var rp interface{}
	//should take timeout as config argument
	var timeout <-chan time.Time
	timeout = time.After(10000 * time.Millisecond)
	select {
	//case when channel has got a call object
	case replyCall := <-client.doneChan:

		if replyCall.Error != nil {
			fmt.Println(replyCall.Error)
			return replyCall.Error, nil

		}
		rp = replyCall.Reply
		//fmt.Println("reply:", *(replyCall.Reply).(*ResponseParameters))
		//encoder := json.NewEncoder(os.Stdout)
		//encoder.Encode(replyCall.Reply)
		//initialize timout
		timeout = time.After(10000 * time.Millisecond)
	case <-timeout:
		fmt.Println("Timed Out")

	}

	return nil, rp
}

func (client *RPCClient) RpcCall(serverInfo ServerInfo, requestMessage string) (error, interface{}) {

	network := serverInfo.Protocol
	address := serverInfo.IpAddress + ":" + strconv.Itoa(serverInfo.Port)
	serverName := serverInfo.ServerID
	
	var response interface{}
	//create new client
	if err := client.NewClient(network, address); err != nil {
		fmt.Println(err)
		return err, response
	}

	//make asychronous calls
	if err := client.CreateAsyncRPC(requestMessage, serverName); err != nil {
		fmt.Println(err)
		return err, response
	}

	//process replies from server
	var err error
	if err, response = client.ProcessReply(); err != nil {
		fmt.Println(err)
		return err, response
	}

	
	return nil, response
}


