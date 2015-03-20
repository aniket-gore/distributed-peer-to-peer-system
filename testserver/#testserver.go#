package main

import(
	"fmt"
	"os"
	"github.com/atulmirajkar/RPC-golang/rpcserver"
)
func main(){

	//read the config file
	configFilePath := os.Args[1]
	configObject := &rpcserver.ConfigType{}
	if err:=configObject.ReadConfig(configFilePath);err!=nil{
		return
	}
	//Initialize server with the config object
	var rpcServer *(rpcserver.RPCServer)
	var err error
	err,rpcServer = rpcserver.GetRPCServerInstance()
	if err!=nil{
		fmt.Println(err)
		return
	}
	rpcServer.InitializeServerConfig(*configObject)
	
	//create the server and listen to incoming connections
	if err:=rpcServer.CreateServer();err!=nil{
		return
	}
	
	
	
}
