package main

import(
	"fmt"
	"os"
	"621_proj/rpcserver"
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
	rpcServer.InitializeChordNode()
	
	//create the server and listen to incoming connections
	if err:=rpcServer.CreateServer();err!=nil{
		return
	}
	
	
	
}
