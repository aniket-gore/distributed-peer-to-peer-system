package rpcserver

import (
	"621_proj/chord"
	"621_proj/hashing"
	"621_proj/rpcclient"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"io/ioutil"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const createdStr = "CREATED"
const accessedStr = "ACCESSED"
const modifiedStr = "MODIFIED"

type ValueWrapper struct {
	Content    interface{} `json:"content"`
	Created    *time.Time  `json:"created,omitempty"`
	Modified   *time.Time  `json:"modified,omitempty"`
	Accessed   *time.Time  `json:"accessed,omitempty"`
	Permission string      `json:"permission,omitempty"`
}

type Dict3 struct {
	Key      string
	Relation string
	Value    ValueWrapper
}

type RequestParameters struct {
	Method string        `json:"method,omitempty"`
	Params []interface{} `json: "params"`
	Id     int           `json: "id,omitempty"`
}

type ResponseParameters struct {
	Result []interface{} `json:"result"`
	Id     int           `json: "id,omitempty"`
	Error  interface{}   `json:"error"`
}

type ResponseParametersInsert struct {
	Result interface{} `json:"result"`
	Id     int         `json: "id,omitempty"`
	Error  interface{} `json:"error"`
}

//can be a file or a database
type PersistentContainerType struct {
	PersistentFilePath string `json:"file"`
}

type ConfigType struct {
	ServerID                   string                  `json:"serverID"`
	Protocol                   string                  `json:"protocol"`
	IpAddress                  string                  `json: "ipAddress"`
	Port                       int                     `json: "port"`
	PersistentStorageContainer PersistentContainerType `json: "persistentStorageContainer"`
	Methods                    []string                `json: "methods"`

	//additional config fields for chord implemetation
	MValue             int    `json: "mvalue"`
	FirstNode          int    `json: "firstnode"`
	LoggerName         string `json: "loggerName"`
	KeyHashLength      int    `json: "keyHashLength"`
	RelationHashLength int    `json: "relationHashLength"`

	//config fields to get default serverinfo we would contact
	ServerIDToContact  string `json:"serverIDToContact"`
	ProtocolToContact  string `json:"protocolToContact"`
	IpAddressToContact string `json: "IpAddressToContact"`
	PortToContact      int    `json: "PortToContact"`

	//Duration to delete
	DurationToDelete string `json:"DurationToDelete"`
}

//this struct object will manage the server
type RPCServer struct {
	configObject ConfigType
	boltDB       *bolt.DB
	stopChan     chan int
	wg           *sync.WaitGroup
	wgLock       *sync.Mutex
	logger       *log.Logger
	logFile      os.File

	//additional fields for chord implementation

	chordNode *(chord.ChordNode)
}

//this struct methods will be exposed to client
type RPCMethod struct {
	rpcServer *RPCServer
}

var rpcServerInstance *RPCServer = nil

func init() {

	rpcServerInstance = &RPCServer{}
	fmt.Println("Server Instance Created")
}

func GetRPCServerInstance() (error, *RPCServer) {
	if rpcServerInstance == nil {
		err := errors.New("Server Instance not created succesfully")
		return err, nil
	}

	return nil, rpcServerInstance
}

func (configObject *ConfigType) ReadConfig(configFilePath string) error {

	file, e := ioutil.ReadFile(configFilePath)
	if e != nil {
		fmt.Println("File Error: %v\n", e)
		return e
	}
	//Unmarshall the json file
	if e := json.Unmarshal(file, configObject); e != nil {
		fmt.Println(e)
		return e
	}

	return nil
}

/*****************************Memory Mapped Persitent FIle Operations using Bolt starts*******************************/

/*
Wraps the content and permission along with the different times
*/
func tripletValueWrapper(tripletValue interface{}, permission string, timeToUpdate string, inputValueWrapper ValueWrapper) ValueWrapper {
	var valueWrapper ValueWrapper

	valueWrapper.Content = tripletValue
	if permission != "" {
		valueWrapper.Permission = permission
	} else {
		valueWrapper.Permission = "RW"
	}
	if timeToUpdate == createdStr {
		valueWrapper.Created = new(time.Time)
		*(valueWrapper.Created) = time.Now()

		valueWrapper.Accessed = inputValueWrapper.Accessed
		valueWrapper.Modified = inputValueWrapper.Modified
	} else if timeToUpdate == accessedStr {
		valueWrapper.Accessed = new(time.Time)
		*(valueWrapper.Accessed) = time.Now()

		valueWrapper.Created = inputValueWrapper.Created
		valueWrapper.Modified = inputValueWrapper.Modified

	} else if timeToUpdate == modifiedStr {
		valueWrapper.Modified = new(time.Time)
		*(valueWrapper.Modified) = time.Now()

		valueWrapper.Accessed = inputValueWrapper.Accessed
		valueWrapper.Created = inputValueWrapper.Created
	}

	return valueWrapper
}

/*
Unmarshal the dict3Value to check the permissions
*/
func (rpcServer *RPCServer) isWriteAllowed(dict3Value []byte) bool {
	var valueWrapper ValueWrapper

	if err := json.Unmarshal(dict3Value, &valueWrapper); err != nil {
		rpcServer.logger.Println("In isWriteAllowed - Unmarshalling error")
		return false
	}

	if valueWrapper.Permission == "R" || valueWrapper.Permission == "r" {
		return false
	}

	return true
}

func (rpcMethod *RPCMethod) insertOrUpdate(reqPar []interface{}) error {

	var parameters []interface{}
	parameters = reqPar

	//added
	var tripletValue interface{}
	var permission string
	//added

	//Use dict3 struct to unmarshall
	dict3 := Dict3{}
	for k, v := range parameters {
		rpcMethod.rpcServer.logger.Println(k, v)
		if k == 0 {
			dict3.Key = v.(string)
		} else if k == 1 {
			dict3.Relation = v.(string)
		} else if k == 2 {
			//dict3.Value = v
			//added
			tripletValue = v
		} else if k == 3 {
			permission = v.(string)
		}
		//added

	}

	//added
	//Read value from db
	var keyPresent bool
	keyPresent = false
	var dict3Value []byte
	var bucket *(bolt.Bucket)
	rpcMethod.rpcServer.boltDB.View(func(tx *bolt.Tx) error {
		bucket = tx.Bucket([]byte(dict3.Key))
		if bucket != nil {
			dict3Value = bucket.Get([]byte(dict3.Relation))
			if dict3Value != nil {
				keyPresent = true

			}
		}

		return nil
	})

	if keyPresent {
		//check if request has arrived from lookup
		//if yes wrap it with the right permission and accesstime
		if err := json.Unmarshal(dict3Value, &(dict3.Value)); err != nil {

			rpcMethod.rpcServer.logger.Println("Value Unmarshalling error ", err, " for id: ", dict3.Key, " ", dict3.Relation)
			return nil
		}
		if permission == accessedStr {
			fmt.Println("Got insertorupdate request from lookup")

			dict3.Value = tripletValueWrapper(tripletValue, dict3.Value.Permission, accessedStr, dict3.Value)
			fmt.Println(dict3.Value)
		} else {
			//check permission on dict3value and return if not allowed
			if !rpcMethod.rpcServer.isWriteAllowed(dict3Value) {
				return nil
			}
			fmt.Println("Got insertorupdate request for existing key relation")
			dict3.Value = tripletValueWrapper(tripletValue, permission, modifiedStr, dict3.Value)
			fmt.Println(dict3.Value)
		}
	} else {
		fmt.Println("Got insertorupdate request for new key relation")
		dict3.Value = tripletValueWrapper(tripletValue, permission, createdStr, *(new(ValueWrapper)))
		fmt.Println(dict3.Value)
	}
	//added

	//Marshal the value and store in db
	valueByte, err := json.Marshal(dict3.Value)
	if err != nil {
		rpcMethod.rpcServer.logger.Println(err)
		return err
	}

	//open db in update mode - insert or update

	rpcMethod.rpcServer.boltDB.Update(func(tx *bolt.Tx) error {

		b, err := tx.CreateBucketIfNotExists([]byte(dict3.Key))
		if err != nil {
			return err
		}

		b = tx.Bucket([]byte(dict3.Key))
		if err = b.Put([]byte(dict3.Relation), valueByte); err != nil {
			return err
		}
		return nil
	})

	return nil

}

func (rpcMethod *RPCMethod) pureinsert(reqPar []interface{}, response *ResponseParametersInsert) error {

	//Unmarshal into array of interfaces
	var parameters []interface{}
	parameters = reqPar

	//Use dict3 struct to unmarshall
	dict3 := Dict3{}
	for k, v := range parameters {
		rpcMethod.rpcServer.logger.Println(k, v)
		if k == 0 {
			dict3.Key = v.(string)
		} else if k == 1 {
			dict3.Relation = v.(string)
		} else if k == 2 {
			//dict3.Value = v.(ValueWrapper)
			valueWrapperBytes, _ := json.Marshal(v)
			if err := json.Unmarshal(valueWrapperBytes, &(dict3.Value)); err != nil {
				rpcMethod.rpcServer.logger.Println(err)
				return err
			}

		}

	}

	//Marshal the value and store in db
	valueByte, err := json.Marshal(dict3.Value)
	if err != nil {
		rpcMethod.rpcServer.logger.Println(err)
		return err
	}

	//open db in view mode
	//check if key already present
	//check if rel already present
	//if both present return err
	//if not open db in update mode and create

	var keyPresent bool
	keyPresent = false
	rpcMethod.rpcServer.boltDB.View(func(tx *bolt.Tx) error {
		//check if key present
		b := tx.Bucket([]byte(dict3.Key))
		if b != nil {
			v := b.Get([]byte(dict3.Relation))
			if v != nil {
				keyPresent = true
			}
		}
		return nil
	})

	//open db in update mode
	if !keyPresent {
		rpcMethod.rpcServer.boltDB.Update(func(tx *bolt.Tx) error {

			b, err := tx.CreateBucketIfNotExists([]byte(dict3.Key))
			if err != nil {
				return err
			}

			b = tx.Bucket([]byte(dict3.Key))
			if err = b.Put([]byte(dict3.Relation), valueByte); err != nil {
				return err
			}
			return nil
		})
		response.Result = "true"
		response.Error = nil

	} else {
		//return an error
		response.Result = nil
		response.Error = "Key Relation already exist"

	}

	return nil
}

func (rpcMethod *RPCMethod) insert(reqPar []interface{}, response *ResponseParametersInsert) error {

	//Unmarshal into array of interfaces
	var parameters []interface{}
	parameters = reqPar

	//added
	var tripletValue interface{}
	var permission string
	//added

	//Use dict3 struct to unmarshall
	dict3 := Dict3{}
	for k, v := range parameters {
		rpcMethod.rpcServer.logger.Println(k, v)
		if k == 0 {
			dict3.Key = v.(string)
		} else if k == 1 {
			dict3.Relation = v.(string)
		} else if k == 2 {
			//dict3.Value = v
			//added
			tripletValue = v
		} else if k == 3 {
			permission = v.(string)
		}
		//added
	}

	//added
	dict3.Value = tripletValueWrapper(tripletValue, permission, createdStr, *(new(ValueWrapper)))
	//added

	//Marshal the value and store in db
	valueByte, err := json.Marshal(dict3.Value)
	if err != nil {
		rpcMethod.rpcServer.logger.Println(err)
		return err
	}

	//open db in view mode
	//check if key already present
	//check if rel already present
	//if both present return err
	//if not open db in update mode and create

	var keyPresent bool
	keyPresent = false
	rpcMethod.rpcServer.boltDB.View(func(tx *bolt.Tx) error {
		//check if key present
		b := tx.Bucket([]byte(dict3.Key))
		if b != nil {
			v := b.Get([]byte(dict3.Relation))
			if v != nil {
				keyPresent = true
			}
		}
		return nil
	})

	//open db in update mode
	if !keyPresent {
		rpcMethod.rpcServer.boltDB.Update(func(tx *bolt.Tx) error {

			b, err := tx.CreateBucketIfNotExists([]byte(dict3.Key))
			if err != nil {
				return err
			}

			b = tx.Bucket([]byte(dict3.Key))
			if err = b.Put([]byte(dict3.Relation), valueByte); err != nil {
				return err
			}
			return nil
		})
		response.Result = "true"
		response.Error = nil

	} else {
		//return an error
		response.Result = nil
		response.Error = "Key Relation already exist"

	}

	return nil
}

func (rpcMethod *RPCMethod) delete(reqPar []interface{}) error {
	var parameters []interface{}
	parameters = reqPar

	//Use dict3 struct to unmarshall
	dict3 := Dict3{}
	// Key string
	// Relation string
	// Value interface{}

	for k, v := range parameters {
		rpcMethod.rpcServer.logger.Println(k, v)
		if k == 0 {
			dict3.Key = v.(string)
		} else if k == 1 {
			dict3.Relation = v.(string)
		}
	}

	//Read value from db
	var keyPresent bool
	keyPresent = false
	var dict3Value []byte
	var bucket *(bolt.Bucket)
	rpcMethod.rpcServer.boltDB.View(func(tx *bolt.Tx) error {
		bucket = tx.Bucket([]byte(dict3.Key))
		if bucket != nil {
			dict3Value = bucket.Get([]byte(dict3.Relation))
			if dict3Value != nil {
				keyPresent = true

			}
		}

		return nil
	})

	rpcMethod.rpcServer.logger.Println(bucket)

	//1. get bucket
	//2. delete relation
	//3. delete if bucket empty - delete bucket
	if keyPresent {

		//added
		//check permission on dict3value and return if not allowed
		if !rpcMethod.rpcServer.isWriteAllowed(dict3Value) {
			return nil
		}
		//added

		rpcMethod.rpcServer.boltDB.Update(func(tx *bolt.Tx) error {

			bucket = tx.Bucket([]byte(dict3.Key))
			bucket.Delete([]byte(dict3.Relation))

			return nil
		})
		//fix empty bucket issue
		//delete bucket if empty
		rpcMethod.rpcServer.boltDB.Update(func(tx *bolt.Tx) error {

			var bucketStats bolt.BucketStats
			bucketStats = tx.Bucket([]byte(dict3.Key)).Stats()
			//if bucket empty delete bucket
			if bucketStats.KeyN == 0 {
				tx.DeleteBucket([]byte(dict3.Key))
			}
			return nil
		})

	}

	return nil
}

//its the same as list buckets
func (rpcMethod *RPCMethod) listKeys(response *ResponseParameters) error {

	//open a read transaction
	rpcMethod.rpcServer.boltDB.View(func(tx *bolt.Tx) error {
		var cursor *bolt.Cursor
		cursor = tx.Cursor()

		//append to reselt the list of buckets
		response.Result = make([]interface{}, 0, 10)
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			rpcMethod.rpcServer.logger.Println("BUCKET ", string(k))
			response.Result = append(response.Result, string(k))
		}

		return nil
	})

	response.Error = nil

	return nil

}

func (rpcMethod *RPCMethod) listIDs(response *ResponseParameters) error {

	//open a read transaction
	rpcMethod.rpcServer.boltDB.View(func(tx *bolt.Tx) error {
		var cursor *bolt.Cursor
		cursor = tx.Cursor()

		var bucket *bolt.Bucket
		response.Result = make([]interface{}, 0, 10)

		//traverse through all keys
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			bucket = tx.Bucket(k)

			//traverse through all relation and value pairs
			bucket.ForEach(func(relation, value []byte) error {
				tuple := make([]string, 2)
				rpcMethod.rpcServer.logger.Println(string(k), string(relation), string(value))
				//make an array of 2 strings [key,relation]
				tuple[0] = string(k)
				tuple[1] = string(relation)
				response.Result = append(response.Result, tuple)
				return nil
			})
		}
		return nil
	})

	response.Error = nil
	return nil

}

func (rpcMethod *RPCMethod) partialLookup(reqPar []interface{}, response *ResponseParameters) error {

	var parameters []interface{}
	parameters = reqPar

	//Use dict3 struct to unmarshall
	dict3 := Dict3{}
	// Key string
	// Relation string
	// Value interface{}

	for k, v := range parameters {
		rpcMethod.rpcServer.logger.Println(k, v)
		if k == 0 {
			dict3.Key = v.(string)
		} else if k == 1 {
			dict3.Relation = v.(string)
		}
	}

	//response is an array of dict3
	response.Result = make([]interface{}, 0, 10)

	//Read value from db
	// var keyPresent bool
	// var dict3Value []byte

	//if key not null
	if dict3.Key != "" {
		rpcMethod.rpcServer.boltDB.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(dict3.Key))

			b.ForEach(func(rel, v []byte) error {
				//fmt.Printf("key=%s, value=%s\n", k, v)
				partialDict3 := Dict3{}
				partialDict3.Key = dict3.Key
				partialDict3.Relation = string(rel)
				if err := json.Unmarshal(v, &(partialDict3.Value)); err != nil {

					rpcMethod.rpcServer.logger.Println("Value Unmarshalling error ", err, " for id: ", partialDict3.Key, " ", partialDict3.Relation)

				}

				response.Result = append(response.Result, partialDict3.Value.Content)
				//added
				//update access time for this lookup
				var reqParToInsertOrUpdate []interface{}
				reqParToInsertOrUpdate = make([]interface{}, 4)
				reqParToInsertOrUpdate[0] = partialDict3.Key
				reqParToInsertOrUpdate[1] = partialDict3.Relation
				reqParToInsertOrUpdate[2] = partialDict3.Value.Content
				//way to tell insert or update has come from lookup
				reqParToInsertOrUpdate[3] = accessedStr
				rpcMethod.insertOrUpdate(reqParToInsertOrUpdate)
				//added

				return nil
			})
			return nil
		})
		//else if relation not null
	} else if dict3.Relation != "" {
		//open a read transaction
		rpcMethod.rpcServer.boltDB.View(func(tx *bolt.Tx) error {
			var cursor *bolt.Cursor
			cursor = tx.Cursor()
			//iterate over each bucket (key)
			for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {

				c := tx.Bucket(k).Cursor()

				//search in the bucket(key) with the given relation as partial prefix
				//faster than a sequential search
				prefix := []byte(dict3.Relation)
				for rel, v := c.Seek(prefix); bytes.HasPrefix(rel, prefix); rel, v = c.Next() {

					//check if the partial prefix is equal to the input relation
					if string(rel) != dict3.Relation {
						continue
					}

					partialDict3 := Dict3{}
					partialDict3.Key = string(k)
					partialDict3.Relation = string(rel)
					if err := json.Unmarshal(v, &(partialDict3.Value)); err != nil {

						rpcMethod.rpcServer.logger.Println("Value Unmarshalling error ", err, " for id: ", partialDict3.Key, " ", partialDict3.Relation)

					}

					response.Result = append(response.Result, partialDict3.Value.Content)
					//added - we are making a call to insertOrUpdate within a view transaction - may fail
					//update access time for this lookup
					// var reqParToInsertOrUpdate []interface{}
					// reqParToInsertOrUpdate = make([]interface{},4)
					// reqParToInsertOrUpdate[0] = partialDict3.Key
					// reqParToInsertOrUpdate[1] = partialDict3.Relation
					// reqParToInsertOrUpdate[2] = partialDict3.Value.Content
					// //way to tell insert or update has come from lookup
					// reqParToInsertOrUpdate[3] = accessedStr
					// rpcMethod.insertOrUpdate(reqParToInsertOrUpdate)
					//added
				} //end for
			}

			return nil
		})

	}

	response.Error = nil

	return nil
}

func (rpcMethod *RPCMethod) lookup(reqPar []interface{}, response *ResponseParameters) error {

	var parameters []interface{}
	parameters = reqPar

	//Use dict3 struct to unmarshall
	dict3 := Dict3{}
	// Key string
	// Relation string
	// Value interface{}

	for k, v := range parameters {
		rpcMethod.rpcServer.logger.Println(k, v)
		if k == 0 {
			dict3.Key = v.(string)
		} else if k == 1 {
			dict3.Relation = v.(string)
		}
	}

	//Read value from db
	var keyPresent bool
	keyPresent = false
	var dict3Value []byte

	rpcMethod.rpcServer.boltDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(dict3.Key))
		if b != nil {
			dict3Value = b.Get([]byte(dict3.Relation))
			if dict3Value != nil {
				keyPresent = true

			}
		}

		return nil
	})
	rpcMethod.rpcServer.logger.Println(dict3Value, keyPresent)

	//if key present unmarshall
	if keyPresent {
		//unmarshall in interface - second argument for unmarshall is a pointer
		if err := json.Unmarshal(dict3Value, &(dict3.Value)); err != nil {

			rpcMethod.rpcServer.logger.Println("Value Unmarshalling error ", err, " for id: ", dict3.Key, " ", dict3.Relation)
			//if error send error
			response.Result = nil
			response.Error = "Unmarshalling Error"

		}
		//save unmarhslled in dict3 Result and Error
		response.Result = make([]interface{}, 3)
		response.Result[0] = dict3.Key
		response.Result[1] = dict3.Relation
		//response.Result[2] = dict3.Value
		//added
		response.Result[2] = dict3.Value.Content
		//added
		response.Error = nil

		//added
		//update access time for this lookup
		var reqParToInsertOrUpdate []interface{}
		reqParToInsertOrUpdate = make([]interface{}, 4)
		reqParToInsertOrUpdate[0] = dict3.Key
		reqParToInsertOrUpdate[1] = dict3.Relation
		reqParToInsertOrUpdate[2] = dict3.Value.Content
		//way to tell insert or update has come from lookup
		reqParToInsertOrUpdate[3] = accessedStr
		rpcMethod.insertOrUpdate(reqParToInsertOrUpdate)
		//added
	} else {
		//if key value not found return false
		rpcMethod.rpcServer.logger.Println("Value not found: ", dict3.Key, " ", dict3.Relation)
		response.Result = nil
		response.Error = "Value not found"

	}

	return nil
}

func (rpcMethod *RPCMethod) shutDown() error {
	rpcMethod.rpcServer.logger.Println(&(rpcMethod.rpcServer.stopChan))
	rpcMethod.rpcServer.logger.Print(*(rpcMethod.rpcServer.wg))
	rpcMethod.rpcServer.logger.Println(" in shutdown")

	rpcMethod.rpcServer.stopChan <- 1
	return nil
}

/*****************************Memory Mapped Persitent FIle Operations using Bolt Ends*******************************/

/*****************************Exposed Wrappers to actual methods start**********************************************/
func (rpcMethod *RPCMethod) PureInsert(jsonInput RequestParameters, jsonOutput *ResponseParametersInsert) error {
	//Initialize rpcserver
	var err error
	err, rpcMethod.rpcServer = GetRPCServerInstance()
	var customError error
	if err != nil {
		customError = errors.New("Getting Server Instance error :" + err.Error())
		rpcMethod.rpcServer.logger.Println(customError)
		return customError
	}

	rpcMethod.rpcServer.wgLock.Lock()
	rpcMethod.rpcServer.wg.Add(1)
	rpcMethod.rpcServer.wgLock.Unlock()

	defer rpcMethod.rpcServer.routineDone()
	rpcMethod.rpcServer.logger.Println(jsonInput.Method)

	//response := new(ResponseParameters)
	response := new(ResponseParametersInsert)

	//make the actual call on the target successor
	if err := rpcMethod.pureinsert(jsonInput.Params, response); err != nil {
		rpcMethod.rpcServer.logger.Println(err)
		response.Result = nil
		response.Error = 1
	}

	//just set ID over here
	//the rest response is set by respective method
	//inserOrUpdate / delete / shutdown does not return anything
	if response != nil {
		response.Id = jsonInput.Id
		*jsonOutput = *response

		rpcMethod.rpcServer.logger.Println("json output: ", *jsonOutput)
		encoder := json.NewEncoder(os.Stdout)
		encoder.Encode(*jsonOutput)

	} else {

		*jsonOutput = ResponseParametersInsert{Result: nil, Id: -1, Error: nil}

	}

	return nil

}

//wrapper to insert
//func (rpcMethod *RPCMethod) Insert(jsonInput RequestParameters, jsonOutput *ResponseParameters) error {
func (rpcMethod *RPCMethod) Insert(jsonInput RequestParameters, jsonOutput *ResponseParametersInsert) error {
	//Initialize rpcserver
	var err error
	err, rpcMethod.rpcServer = GetRPCServerInstance()
	var customError error
	if err != nil {
		customError = errors.New("Getting Server Instance error :" + err.Error())
		rpcMethod.rpcServer.logger.Println(customError)
		return customError
	}

	rpcMethod.rpcServer.wgLock.Lock()
	rpcMethod.rpcServer.wg.Add(1)
	rpcMethod.rpcServer.wgLock.Unlock()

	defer rpcMethod.rpcServer.routineDone()
	rpcMethod.rpcServer.logger.Println(jsonInput.Method)

	//response := new(ResponseParameters)
	response := new(ResponseParametersInsert)

	/**********************get Successor for the insert**********************/
	var parameters []interface{}
	parameters = jsonInput.Params

	//Use dict3 struct to unmarshall
	var key string
	var relation string
	for k, v := range parameters {

		if k == 0 {
			key = v.(string)
		} else if k == 1 {
			relation = v.(string)
		}
	}

	var finalChordID uint32
	finalChordID = rpcMethod.rpcServer.chordNode.GetHashFromKeyAndValue(key, relation)

	var successorInfo chord.ServerInfoWithID
	successorInfo, err = rpcMethod.rpcServer.chordNode.GetSuccessorInfoForInputHash(finalChordID)
	if err != nil {
		rpcMethod.rpcServer.logger.Println(err)
		return err
	}
	//for the target successor  - ID returned will be the same as chordNode.Id
	if successorInfo.Id != rpcMethod.rpcServer.chordNode.Id {

		jsonBytes, err := json.Marshal(jsonInput)
		if err != nil {
			rpcMethod.rpcServer.logger.Println(err)
			return err
		}

		var responseTemp interface{}
		client := &rpcclient.RPCClient{}
		err, responseTemp = client.RpcCall(successorInfo.ServerInfo, string(jsonBytes))

		if err != nil {
			rpcMethod.rpcServer.logger.Println(err)
			return nil
		}

		response.Result = responseTemp.(*rpcclient.ResponseParametersInsert).Result
		response.Error = responseTemp.(*rpcclient.ResponseParametersInsert).Error

		/*********************get Successor for the insert - ends*********************/
	} else {

		//make the actual call on the target successor
		if err := rpcMethod.insert(jsonInput.Params, response); err != nil {
			rpcMethod.rpcServer.logger.Println(err)
			response.Result = nil
			response.Error = 1
		}
	}
	//just set ID over here
	//the rest response is set by respective method
	//inserOrUpdate / delete / shutdown does not return anything
	if response != nil {
		response.Id = jsonInput.Id
		*jsonOutput = *response

		rpcMethod.rpcServer.logger.Println("json output: ", *jsonOutput)
		encoder := json.NewEncoder(os.Stdout)
		encoder.Encode(*jsonOutput)

	} else {

		*jsonOutput = ResponseParametersInsert{Result: nil, Id: -1, Error: nil}

	}

	return nil

}

//wrapper to insertorupdate
func (rpcMethod *RPCMethod) InsertOrUpdate(jsonInput RequestParameters, jsonOutput *ResponseParameters) error {
	//Initialize rpcserver
	var err error
	err, rpcMethod.rpcServer = GetRPCServerInstance()
	var customError error
	if err != nil {
		customError = errors.New("Getting Server Instance error :" + err.Error())
		rpcMethod.rpcServer.logger.Println(customError)
		return customError
	}

	rpcMethod.rpcServer.wgLock.Lock()
	rpcMethod.rpcServer.wg.Add(1)
	rpcMethod.rpcServer.wgLock.Unlock()

	defer rpcMethod.rpcServer.routineDone()
	rpcMethod.rpcServer.logger.Println(jsonInput.Method)

	response := new(ResponseParameters)

	/*******************get Successor for the insertorupdate******************/
	var parameters []interface{}
	parameters = jsonInput.Params

	//Use dict3 struct to unmarshall
	var key string
	var relation string
	for k, v := range parameters {

		if k == 0 {
			key = v.(string)
		} else if k == 1 {
			relation = v.(string)
		}
	}

	var finalChordID uint32
	finalChordID = rpcMethod.rpcServer.chordNode.GetHashFromKeyAndValue(key, relation)

	var successorInfo chord.ServerInfoWithID
	successorInfo, err = rpcMethod.rpcServer.chordNode.GetSuccessorInfoForInputHash(finalChordID)

	if err != nil {
		rpcMethod.rpcServer.logger.Println(err)
		return err
	}
	//for the target successor  - ID returned will be the same as chordNode.Id
	if successorInfo.Id != rpcMethod.rpcServer.chordNode.Id {

		jsonBytes, err := json.Marshal(jsonInput)
		if err != nil {
			rpcMethod.rpcServer.logger.Println(err)
			return err
		}

		var responseTemp interface{}
		client := &rpcclient.RPCClient{}
		err, responseTemp = client.RpcCall(successorInfo.ServerInfo, string(jsonBytes))

		if err != nil {
			rpcMethod.rpcServer.logger.Println(err)
			return nil
		}

		response.Result = responseTemp.(*rpcclient.ResponseParameters).Result
		response.Error = responseTemp.(*rpcclient.ResponseParameters).Error

		/***************get Successor for the insert - ends*********************/
	} else {

		if err := rpcMethod.insertOrUpdate(jsonInput.Params); err != nil {
			//even though error, we are not returning anything
			rpcMethod.rpcServer.logger.Println(err)
		}
	}
	//no response
	response = nil

	//just set ID over here
	//the rest response is set by respective method
	//inserOrUpdate / delete / shutdown does not return anything
	if response != nil {
		response.Id = jsonInput.Id
		*jsonOutput = *response

		rpcMethod.rpcServer.logger.Println("json output: ", *jsonOutput)
		encoder := json.NewEncoder(os.Stdout)
		encoder.Encode(*jsonOutput)

	} else {

		*jsonOutput = ResponseParameters{Result: nil, Id: -1, Error: nil}

	}

	return nil

}

//wrapper to delete
//no response
func (rpcMethod *RPCMethod) Delete(jsonInput RequestParameters, jsonOutput *ResponseParameters) error {
	//Initialize rpcserver
	var err error
	err, rpcMethod.rpcServer = GetRPCServerInstance()
	var customError error
	if err != nil {
		customError = errors.New("Getting Server Instance error :" + err.Error())
		rpcMethod.rpcServer.logger.Println(customError)
		return customError
	}

	rpcMethod.rpcServer.wgLock.Lock()
	rpcMethod.rpcServer.wg.Add(1)
	rpcMethod.rpcServer.wgLock.Unlock()

	defer rpcMethod.rpcServer.routineDone()
	rpcMethod.rpcServer.logger.Println(jsonInput.Method)

	response := new(ResponseParameters)
	/***************************get Successor for the delete***********************/
	var parameters []interface{}
	parameters = jsonInput.Params

	//Use dict3 struct to unmarshall
	var key string
	var relation string
	for k, v := range parameters {

		if k == 0 {
			key = v.(string)
		} else if k == 1 {
			relation = v.(string)
		}
	}

	var finalChordID uint32
	finalChordID = rpcMethod.rpcServer.chordNode.GetHashFromKeyAndValue(key, relation)

	var successorInfo chord.ServerInfoWithID
	successorInfo, err = rpcMethod.rpcServer.chordNode.GetSuccessorInfoForInputHash(finalChordID)

	if err != nil {
		rpcMethod.rpcServer.logger.Println(err)
		return err
	}
	//for the target successor  - ID returned will be the same as chordNode.Id
	if successorInfo.Id != rpcMethod.rpcServer.chordNode.Id {

		jsonBytes, err := json.Marshal(jsonInput)
		if err != nil {
			rpcMethod.rpcServer.logger.Println(err)
			return err
		}

		var responseTemp interface{}
		client := &rpcclient.RPCClient{}
		err, responseTemp = client.RpcCall(successorInfo.ServerInfo, string(jsonBytes))

		if err != nil {
			rpcMethod.rpcServer.logger.Println(err)
			return nil
		}

		response.Result = responseTemp.(*rpcclient.ResponseParameters).Result
		response.Error = responseTemp.(*rpcclient.ResponseParameters).Error

		/*******************get Successor for the delete - ends**********************/
	} else {

		if err := rpcMethod.delete(jsonInput.Params); err != nil {
			rpcMethod.rpcServer.logger.Println(err)
			response.Result = nil
			response.Error = 1
		}
	}

	//no response
	response = nil

	//just set ID over here
	//the rest response is set by respective method
	//inserOrUpdate / delete / shutdown does not return anything
	if response != nil {
		response.Id = jsonInput.Id
		*jsonOutput = *response

		rpcMethod.rpcServer.logger.Println("json output: ", *jsonOutput)
		encoder := json.NewEncoder(os.Stdout)
		encoder.Encode(*jsonOutput)

	} else {

		*jsonOutput = ResponseParameters{Result: nil, Id: -1, Error: nil}

	}

	return nil

}

//wrapper to shutdown
func (rpcMethod *RPCMethod) Shutdown(jsonInput RequestParameters, jsonOutput *ResponseParameters) error {
	var err error
	err, rpcMethod.rpcServer = GetRPCServerInstance()
	var customError error
	if err != nil {
		customError = errors.New("Getting Server Instance error :" + err.Error())
		rpcMethod.rpcServer.logger.Println(customError)
		return customError
	}

	rpcMethod.rpcServer.wgLock.Lock()
	rpcMethod.rpcServer.wg.Add(1)
	rpcMethod.rpcServer.wgLock.Unlock()

	defer rpcMethod.rpcServer.routineDone()
	rpcMethod.rpcServer.logger.Println(jsonInput.Method)

	//n may notify its predecessor p and successor s before leaving
	rpcMethod.rpcServer.chordNode.NotifyShutDownToRing()

	//transfer keys to successor if successor not set to itself
	if rpcMethod.rpcServer.chordNode.FingerTable[1] != rpcMethod.rpcServer.chordNode.Id {
		rpcMethod.rpcServer.makeInsertsToSuccessor()
		//delete all buckets from own DB
		rpcMethod.rpcServer.deleteAllBuckets()

	}

	response := new(ResponseParameters)

	if err := rpcMethod.shutDown(); err != nil {
		response.Result = nil
		response.Error = 1

	}

	//no response
	response = nil

	//just set ID over here
	//the rest response is set by respective method
	//inserOrUpdate / delete / shutdown does not return anything
	if response != nil {
		response.Id = jsonInput.Id
		*jsonOutput = *response

		rpcMethod.rpcServer.logger.Println("json output: ", *jsonOutput)
		encoder := json.NewEncoder(os.Stdout)
		encoder.Encode(*jsonOutput)

	} else {

		*jsonOutput = ResponseParameters{Result: nil, Id: -1, Error: nil}

	}

	return nil

}

//wrapper to listkeys
func (rpcMethod *RPCMethod) ListKeys(jsonInput RequestParameters, jsonOutput *ResponseParameters) error {
	var err error
	err, rpcMethod.rpcServer = GetRPCServerInstance()
	var customError error
	if err != nil {
		customError = errors.New("Getting Server Instance error :" + err.Error())
		rpcMethod.rpcServer.logger.Println(customError)
		return customError
	}

	rpcMethod.rpcServer.wgLock.Lock()
	rpcMethod.rpcServer.wg.Add(1)
	rpcMethod.rpcServer.wgLock.Unlock()

	defer rpcMethod.rpcServer.routineDone()
	rpcMethod.rpcServer.logger.Println(jsonInput.Method)

	jsonInputObject := new(RequestParameters)
	requestParams := make([]interface{}, 1, 1)
	requestParams[0] = rpcMethod.rpcServer.chordNode.Id

	jsonInputObject.Params = requestParams
	jsonInputObject.Method = "listKeysWithSrcNodeID"

	jsonBytes, err := json.Marshal(jsonInputObject)
	if err != nil {
		rpcMethod.rpcServer.logger.Println(err)
		return err
	}

	response := new(ResponseParameters)

	rpcMethod.rpcServer.logger.Println("In listKeys:")
	//prepare server info
	clientServerInfo, err := rpcMethod.rpcServer.chordNode.PrepareClientServerInfo(rpcMethod.rpcServer.chordNode.FingerTable[1])
	if err == nil {

		// get the keys from successor nodes
		var responseFromSuccessor interface{}
		client := &rpcclient.RPCClient{}
		err, responseFromSuccessor = client.RpcCall(clientServerInfo, string(jsonBytes))

		if err != nil {
			rpcMethod.rpcServer.logger.Println(err)
		}

		// get the keys from current node
		if err := rpcMethod.listKeys(response); err != nil {
			response.Result = nil
			response.Error = 1
		}

		// Merge the response arrays of keys
		if responseFromSuccessor != nil {
			mergedResponse := append(responseFromSuccessor.(*rpcclient.ResponseParameters).Result, response.Result...)
			response.Result = mergedResponse
		}

	} else {
		rpcMethod.rpcServer.logger.Println(err)
	}

	//just set ID over here
	//the rest response is set by respective method
	//inserOrUpdate / delete / shutdown does not return anything

	if response != nil {
		response.Id = jsonInput.Id
		*jsonOutput = *response

		rpcMethod.rpcServer.logger.Println("json output: ", *jsonOutput)
		encoder := json.NewEncoder(os.Stdout)
		encoder.Encode(*jsonOutput)

	} else {

		*jsonOutput = ResponseParameters{Result: nil, Id: -1, Error: nil}

	}
	return nil

}

//wrapper to listkeys
func (rpcMethod *RPCMethod) ListKeysWithSrcNodeID(jsonInput RequestParameters, jsonOutput *ResponseParameters) error {
	var err error
	err, rpcMethod.rpcServer = GetRPCServerInstance()
	var customError error
	if err != nil {
		customError = errors.New("Getting Server Instance error :" + err.Error())
		rpcMethod.rpcServer.logger.Println(customError)
		return customError
	}

	rpcMethod.rpcServer.wgLock.Lock()
	rpcMethod.rpcServer.wg.Add(1)
	rpcMethod.rpcServer.wgLock.Unlock()

	defer rpcMethod.rpcServer.routineDone()

	response := new(ResponseParameters)

	rpcMethod.rpcServer.logger.Println("In listKeysWithNodeID:")

	if uint32(jsonInput.Params[0].(float64)) != rpcMethod.rpcServer.chordNode.Id {
		// FORWARD REQUEST
		// response := new(ResponseParameters)
		//prepare server info
		clientServerInfo, err := rpcMethod.rpcServer.chordNode.PrepareClientServerInfo(rpcMethod.rpcServer.chordNode.FingerTable[1])
		if err == nil {

			// get the keys from successor nodes
			jsonBytes, err := json.Marshal(jsonInput)
			if err != nil {
				rpcMethod.rpcServer.logger.Println(err)
				return err
			}
			var responseFromSuccessor interface{}
			client := &rpcclient.RPCClient{}
			err, responseFromSuccessor = client.RpcCall(clientServerInfo, string(jsonBytes))

			if err != nil {
				rpcMethod.rpcServer.logger.Println(err)
			}

			// get the keys from current node
			if err := rpcMethod.listKeys(response); err != nil {
				response.Result = nil
				response.Error = 1
			}

			// Merge the response arrays of keys
			// Merge the response arrays of keys
			if responseFromSuccessor != nil {
				mergedResponse := append(responseFromSuccessor.(*rpcclient.ResponseParameters).Result, response.Result...)
				response.Result = mergedResponse
			}

		} else {
			rpcMethod.rpcServer.logger.Println(err)
		}
	}

	//just set ID over here
	//the rest response is set by respective method
	//inserOrUpdate / delete / shutdown does not return anything

	if response != nil {
		response.Id = jsonInput.Id
		*jsonOutput = *response

		rpcMethod.rpcServer.logger.Println("json output: ", *jsonOutput)
		encoder := json.NewEncoder(os.Stdout)
		encoder.Encode(*jsonOutput)

	} else {

		*jsonOutput = ResponseParameters{Result: nil, Id: -1, Error: nil}

	}

	return nil

}

//wrapper to listIDs
func (rpcMethod *RPCMethod) ListIDs(jsonInput RequestParameters, jsonOutput *ResponseParameters) error {
	//Initialize rpcserver
	var err error
	err, rpcMethod.rpcServer = GetRPCServerInstance()
	var customError error
	if err != nil {
		customError = errors.New("Getting Server Instance error :" + err.Error())
		rpcMethod.rpcServer.logger.Println(customError)
		return customError
	}

	rpcMethod.rpcServer.wgLock.Lock()
	rpcMethod.rpcServer.wg.Add(1)
	rpcMethod.rpcServer.wgLock.Unlock()

	defer rpcMethod.rpcServer.routineDone()
	rpcMethod.rpcServer.logger.Println(jsonInput.Method)

	response := new(ResponseParameters)

	if err := rpcMethod.listIDs(response); err != nil {
		rpcMethod.rpcServer.logger.Println(err)
		response.Result = nil
		response.Error = 1
	}

	//just set ID over here
	//the rest response is set by respective method
	//inserOrUpdate / delete / shutdown does not return anything
	if response != nil {
		response.Id = jsonInput.Id
		*jsonOutput = *response

		rpcMethod.rpcServer.logger.Println("json output: ", *jsonOutput)
		encoder := json.NewEncoder(os.Stdout)
		encoder.Encode(*jsonOutput)

	} else {

		*jsonOutput = ResponseParameters{Result: nil, Id: -1, Error: nil}

	}

	return nil

}

//wrapper to partialLookup
func (rpcMethod *RPCMethod) PartialLookup(jsonInput RequestParameters, jsonOutput *ResponseParameters) error {
	//Initialize rpcserver
	var err error
	err, rpcMethod.rpcServer = GetRPCServerInstance()
	var customError error
	if err != nil {
		customError = errors.New("Getting Server Instance error :" + err.Error())
		rpcMethod.rpcServer.logger.Println(customError)
		return customError
	}

	rpcMethod.rpcServer.wgLock.Lock()
	rpcMethod.rpcServer.wg.Add(1)
	rpcMethod.rpcServer.wgLock.Unlock()

	defer rpcMethod.rpcServer.routineDone()
	rpcMethod.rpcServer.logger.Println(jsonInput)

	response := new(ResponseParameters)

	if err := rpcMethod.partialLookup(jsonInput.Params, response); err != nil {
		rpcMethod.rpcServer.logger.Println(err)
		response.Result = nil
		response.Error = 1

	}

	//just set ID over here
	//the rest response is set by respective method
	//inserOrUpdate / delete / shutdown does not return anything
	if response != nil {
		response.Id = jsonInput.Id
		*jsonOutput = *response

		rpcMethod.rpcServer.logger.Println("json output: ", *jsonOutput)
		encoder := json.NewEncoder(os.Stdout)
		encoder.Encode(*jsonOutput)

	} else {

		*jsonOutput = ResponseParameters{Result: nil, Id: -1, Error: nil}

	}

	return nil

}

//wrapper to lookup
func (rpcMethod *RPCMethod) Lookup(jsonInput RequestParameters, jsonOutput *ResponseParameters) error {
	//Initialize rpcserver
	var err error
	err, rpcMethod.rpcServer = GetRPCServerInstance()
	var customError error
	if err != nil {
		customError = errors.New("Getting Server Instance error :" + err.Error())
		rpcMethod.rpcServer.logger.Println(customError)
		return customError
	}

	rpcMethod.rpcServer.wgLock.Lock()
	rpcMethod.rpcServer.wg.Add(1)
	rpcMethod.rpcServer.wgLock.Unlock()

	defer rpcMethod.rpcServer.routineDone()
	rpcMethod.rpcServer.logger.Println(jsonInput)

	response := new(ResponseParameters)

	var responseTemp interface{}

	/*
		if err := rpcMethod.lookup(jsonInput.Params, response); err != nil {
			rpcMethod.rpcServer.logger.Println(err)
			response.Result = nil
			response.Error = 1

		}
	*/

	err, responseTemp = rpcMethod.checkIfPartialAndForwardRequest(jsonInput)

	if err != nil {
		rpcMethod.rpcServer.logger.Println(err)
		return err
	}

	response.Result = responseTemp.(ResponseParameters).Result
	response.Error = responseTemp.(ResponseParameters).Error

	//just set ID over here
	//the rest response is set by respective method
	//inserOrUpdate / delete / shutdown does not return anything
	if response != nil {
		response.Id = jsonInput.Id
		*jsonOutput = *response

		rpcMethod.rpcServer.logger.Println("json output: ", *jsonOutput)
		encoder := json.NewEncoder(os.Stdout)
		encoder.Encode(*jsonOutput)

	} else {

		*jsonOutput = ResponseParameters{Result: nil, Id: -1, Error: nil}

	}

	return nil

}

/*****************************Exposed Wrappers to actual methods Ends**********************************************/

/*****************************Server Helper Routines start**********************************************/

func (rpcServer *RPCServer) routineDone() {
	rpcServer.wgLock.Lock()
	rpcServer.wg.Done()
	rpcServer.wgLock.Unlock()

}

func (rpcServer *RPCServer) InitializeServerConfig(inputConfigObject ConfigType) error {

	//initialize config
	rpcServer.configObject = inputConfigObject

	//initialize channel
	//sender gets blocked gets
	//rpcServer.stopChan = make(chan int)
	//to make single sender unblocking
	rpcServer.stopChan = make(chan int, 1)
	fmt.Println("Initialized Config to Server")

	rpcServer.wg = &sync.WaitGroup{}
	rpcServer.wgLock = &sync.Mutex{}

	//intialize logger
	file, e := os.Create(inputConfigObject.LoggerName)
	if e != nil {
		fmt.Println("File Error: %v\n", e)
		return e
	}

	rpcServer.logger = log.New(file, "log: ", log.LstdFlags)
	rpcServer.logger.SetFlags(log.LstdFlags | log.Lshortfile)
	return nil
}

func (rpcServer *RPCServer) deleteAllBuckets() {

	//open a read transaction
	rpcServer.boltDB.Update(func(tx *bolt.Tx) error {
		var cursor *bolt.Cursor
		cursor = tx.Cursor()

		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			if err := tx.DeleteBucket(k); err != nil {
				rpcServer.logger.Println(err)
			}

		}

		return nil
	})

}

func (rpcServer *RPCServer) closeServerAndDB(listener net.Listener) error {
	<-rpcServer.stopChan

	(rpcServer.wg).Wait()

	rpcServer.logger.Println("Closing Connection")
	listener.Close()

	//close logger
	rpcServer.logFile.Close()

	//once all connections are served close db and return
	rpcServer.boltDB.Close()

	fmt.Println("Server Connection closing")
	fmt.Println("DB Connection closing")
	var err error
	err = errors.New("Stop Server")
	return err

}

func (rpcServer *RPCServer) CreateServer() error {

	//register method
	rpcServer.logger.Println("In createserver")

	rpcServer.logger.Println(rpcServer.configObject.ServerID)
	if err := rpc.RegisterName(rpcServer.configObject.ServerID, new(RPCMethod)); err != nil {
		rpcServer.logger.Println(err)
		fmt.Println(err)
		return err

	}

	rpcServer.logger.Println(rpcServer.configObject.Protocol, ":"+strconv.Itoa(rpcServer.configObject.Port))
	tcpAddr, err := net.ResolveTCPAddr(rpcServer.configObject.Protocol, ":"+strconv.Itoa(rpcServer.configObject.Port))
	if err != nil {
		rpcServer.logger.Println(err)
		fmt.Println(err)
		return err
	}

	//listen on port
	listener, err := net.ListenTCP(rpcServer.configObject.Protocol, tcpAddr)
	if err != nil {
		rpcServer.logger.Println(err)
		fmt.Println(err)
		return err
	}

	//intialize db
	//var err error
	rpcServer.boltDB, err = bolt.Open(rpcServer.configObject.PersistentStorageContainer.PersistentFilePath, 0600, nil)
	if err != nil {
		fmt.Println(err)
		return err
	}

	//asynchronously start a methd and listen on channel
	go rpcServer.closeServerAndDB(listener)
	//infinite for to listen requests

	//asynchronously call Initialize Chord Node
	go rpcServer.InitializeChordNode()

	for {

		conn, err := listener.Accept()
		if err != nil {
			rpcServer.logger.Println(err)
			return err
		}

		go jsonrpc.ServeConn(conn)

	}

	return nil
}

/*****************************Server Helper Routines Ends**********************************************/
/*****************************Chord related functions**************************************************/

func (rpcServer *RPCServer) InitializeChordNode() {

	rpcServer.logger.Println("In Initialize Chord Node")
	rpcServer.chordNode = &chord.ChordNode{}
	rpcServer.chordNode.MValue = rpcServer.configObject.MValue
	rpcServer.chordNode.FirstNode = rpcServer.configObject.FirstNode
	rpcServer.chordNode.Logger = rpcServer.logger
	rpcServer.chordNode.KeyHashLength = rpcServer.configObject.KeyHashLength
	rpcServer.chordNode.RelationHashLength = rpcServer.configObject.RelationHashLength

	rpcServer.chordNode.MyServerInfo.ServerID = rpcServer.configObject.ServerID
	rpcServer.chordNode.MyServerInfo.Protocol = rpcServer.configObject.Protocol
	rpcServer.chordNode.MyServerInfo.IpAddress = rpcServer.configObject.IpAddress
	rpcServer.chordNode.MyServerInfo.Port = rpcServer.configObject.Port

	rpcServer.chordNode.ToContactServerInfo.ServerID = rpcServer.configObject.ServerIDToContact
	rpcServer.chordNode.ToContactServerInfo.Protocol = rpcServer.configObject.ProtocolToContact
	rpcServer.chordNode.ToContactServerInfo.IpAddress = rpcServer.configObject.IpAddressToContact
	rpcServer.chordNode.ToContactServerInfo.Port = rpcServer.configObject.PortToContact

	var err error
	rpcServer.chordNode.DurationToDelete, err = time.ParseDuration(rpcServer.configObject.DurationToDelete)
	if err != nil {
		fmt.Println("Error parsing time duration")
		return
	}

	rpcServer.chordNode.InitializeNode()
	rpcServer.chordNode.RunBackgroundProcesses()

}

/*
request <-"{"method":"findSuccessor","params":[22]}"
response <- "{"result":[23],"id":,"error":null }"
*/

func (rpcMethod *RPCMethod) FindSuccessor(jsonInput RequestParameters, jsonOutput *ResponseParameters) error {

	//Initialize rpcserver
	var err error
	err, rpcMethod.rpcServer = GetRPCServerInstance()
	var customError error
	if err != nil {
		customError = errors.New("Getting Server Instance error :" + err.Error())
		rpcMethod.rpcServer.logger.Println(customError)
		return customError
	}

	rpcMethod.rpcServer.logger.Println("RPCCall: In FindSuccessor ")
	rpcMethod.rpcServer.logger.Println("Input=", jsonInput)
	defer rpcMethod.rpcServer.logger.Println("Exited FindSuccssor=")

	var inputId uint32
	var succId uint32
	var interId uint32

	succServerInfo := rpcclient.ServerInfo{}

	var parameters []interface{}
	parameters = jsonInput.Params

	//get inputId from the []interface
	for k, v := range parameters {
		if k == 0 {
			inputId = uint32(v.(float64))
		}
	}

	/*
	   if (id ∈ (n, successor])
	      return successor;
	*/
	rpcMethod.rpcServer.logger.Println("Find Successor of (id)" + fmt.Sprint(inputId))
	rpcMethod.rpcServer.logger.Println("Current Node (n):" + fmt.Sprint(rpcMethod.rpcServer.chordNode.Id))
	rpcMethod.rpcServer.logger.Println("Current Node Successor (successor):" + fmt.Sprint(rpcMethod.rpcServer.chordNode.Successor))

	//case when only 1 node in chord ring - that node will be the successor
	if rpcMethod.rpcServer.chordNode.Id == rpcMethod.rpcServer.chordNode.Successor {
		succId = rpcMethod.rpcServer.chordNode.Id

		//basic condition
	} else if rpcMethod.rpcServer.chordNode.Id < inputId && inputId <= rpcMethod.rpcServer.chordNode.Successor {
		succId = rpcMethod.rpcServer.chordNode.Successor

		//successor id is less than node id - check whether inputId falls between (n,sucessor + 2^m)

	} else if rpcMethod.rpcServer.chordNode.Successor < rpcMethod.rpcServer.chordNode.Id && (inputId > rpcMethod.rpcServer.chordNode.Id || inputId < rpcMethod.rpcServer.chordNode.Successor) {
		succId = rpcMethod.rpcServer.chordNode.Successor

	} else {
		interId = rpcMethod.rpcServer.chordNode.ClosestPrecedingNode(inputId)

		// if call is being forwarded to the current node itself, that means current node itself is the successor
		if interId == rpcMethod.rpcServer.chordNode.Id {
			succId = rpcMethod.rpcServer.chordNode.Id
		} else {

			//create rpc call "{"method":"findSuccessor","params":[inputId]}"
			jsonMessage := "{\"method\":\"findSuccessor\",\"params\":[" + fmt.Sprint(inputId) + "]}"

			clientServerInfo := rpcclient.ServerInfo{}
			clientServerInfo.ServerID = rpcMethod.rpcServer.chordNode.FtServerMapping[interId].ServerID
			clientServerInfo.Protocol = rpcMethod.rpcServer.chordNode.FtServerMapping[interId].Protocol
			clientServerInfo.IpAddress = rpcMethod.rpcServer.chordNode.FtServerMapping[interId].IpAddress
			clientServerInfo.Port = rpcMethod.rpcServer.chordNode.FtServerMapping[interId].Port

			client := &rpcclient.RPCClient{}
			err, response := client.RpcCall(clientServerInfo, jsonMessage)

			if err != nil {
				fmt.Println(err)
				return nil
			}

			jsonOutput.Result = make([]interface{}, 2)
			// process only if response is present
			if response.(*(rpcclient.ResponseParameters)).Result != nil {
				succId = uint32(response.(*(rpcclient.ResponseParameters)).Result[0].(float64))

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
				succServerInfo = resultServerInfo

				//insert successor ID into jsonOutput
				jsonOutput.Result[0] = succId

				//insert succId's serverInfo into jsonOutput
				jsonOutput.Result[1] = succServerInfo
			}
			rpcMethod.rpcServer.logger.Println("Result from findSucc=", jsonOutput.Result)
			return nil
		} // END-ELSE
	}

	jsonOutput.Result = make([]interface{}, 2)
	//insert successor ID into jsonOutput
	jsonOutput.Result[0] = succId

	//insert succId's serverInfo into jsonOutput
	if rpcMethod.rpcServer.chordNode.Id == succId {
		jsonOutput.Result[1] = rpcMethod.rpcServer.chordNode.MyServerInfo
	} else {
		jsonOutput.Result[1] = rpcMethod.rpcServer.chordNode.FtServerMapping[succId]
	}
	return nil
}

/*
request <-"{"method":"GetPredecessor","params":[]}"
response <- "{"result":[true,12345678],"id":,"error":null }"
*/

func (rpcMethod *RPCMethod) GetPredecessor(jsonInput RequestParameters, jsonOutput *ResponseParameters) error {

	//Initialize rpcserver
	var err error
	err, rpcMethod.rpcServer = GetRPCServerInstance()
	var customError error
	if err != nil {
		customError = errors.New("Getting Server Instance error :" + err.Error())
		rpcMethod.rpcServer.logger.Println(customError)
		return customError
	}

	rpcMethod.rpcServer.logger.Println("RPCCall: In GetPredecessor")
	rpcMethod.rpcServer.logger.Println("Input=", jsonInput)

	isPredecessorNil, predecessor := rpcMethod.rpcServer.chordNode.GetPredecessor()

	predecessorServerInfo := rpcclient.ServerInfo{}
	if !isPredecessorNil {
		predecessorServerInfo = rpcMethod.rpcServer.chordNode.FtServerMapping[predecessor]
	}

	jsonOutput.Result = make([]interface{}, 3)
	jsonOutput.Result[0] = isPredecessorNil
	jsonOutput.Result[1] = predecessor
	jsonOutput.Result[2] = predecessorServerInfo

	rpcMethod.rpcServer.logger.Println("jsonOutputResult=", jsonOutput.Result)
	return nil
}

/*
request <-"{"method":"Notify","params":[10,{"ServerID":"9999","Protocol":"tcp","IpAddress":"127.0.0.1","Port":1235}]}"
response <- "{"result":[],"id":,"error":null }"
*/
func (rpcMethod *RPCMethod) Notify(jsonInput RequestParameters, jsonOutput *ResponseParameters) error {

	//Initialize rpcserver
	var err error
	err, rpcMethod.rpcServer = GetRPCServerInstance()
	var customError error
	if err != nil {
		customError = errors.New("Getting Server Instance error :" + err.Error())
		rpcMethod.rpcServer.logger.Println(customError)
		return customError
	}

	rpcMethod.rpcServer.logger.Println("RPCCall: In Notify")
	rpcMethod.rpcServer.logger.Println("Input=", jsonInput)

	var probablePredecessorId uint32
	probablePredecessorServerInfo := rpcclient.ServerInfo{}
	var parameters []interface{}
	parameters = jsonInput.Params

	//get inputId from the []interface
	for k, v := range parameters {
		if k == 0 {
			probablePredecessorId = uint32(v.(float64))
		} else if k == 1 {
			resultServerInfo := rpcclient.ServerInfo{}
			for key, value := range v.(map[string]interface{}) {
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
			probablePredecessorServerInfo = resultServerInfo
		}
	}

	//probablePredecessor ∈ (predecessor, chordNode)
	isPredecessorNil, predecessor := rpcMethod.rpcServer.chordNode.GetPredecessor()
	//predecessor == chordNode.Id refers to the case where the ActualNodesInRing = 1 i.e. predecessor is the node itself
	nodeId := rpcMethod.rpcServer.chordNode.Id
	if isPredecessorNil ||
		(probablePredecessorId > predecessor && probablePredecessorId < nodeId) ||
		predecessor == nodeId ||
		(probablePredecessorId < nodeId && predecessor > probablePredecessorId && predecessor > nodeId) ||
		(probablePredecessorId > nodeId && predecessor < probablePredecessorId && predecessor > nodeId) {

		rpcMethod.rpcServer.logger.Println("New Predecessor or Predecessor change - setPredecessor")
		rpcMethod.rpcServer.chordNode.SetPredecessor(false, probablePredecessorId)
		rpcMethod.rpcServer.chordNode.FtServerMapping[rpcMethod.rpcServer.chordNode.Predecessor] = probablePredecessorServerInfo

	}

	return nil
}

/*
request <-"{"method":"CheckPredecessor","params":[]}"
response <- "{"result":[true],"id":,"error":null }"
*/
func (rpcMethod *RPCMethod) CheckPredecessor(jsonInput RequestParameters, jsonOutput *ResponseParameters) error {

	jsonOutput.Result = make([]interface{}, 1)
	jsonOutput.Result[0] = true

	return nil
}

/*
set new Predecessor when predecessor has left
request <-"{"method":"PredecessorLeft","params":[PredecessorServerinfo_object]}"
response <- "{"result":[true],"id":,"error":null }"
*/
func (rpcMethod *RPCMethod) PredecessorLeft(jsonInput RequestParameters, jsonOutput *ResponseParameters) error {

	//Initialize rpcserver
	var err error
	err, rpcMethod.rpcServer = GetRPCServerInstance()
	var customError error
	if err != nil {
		customError = errors.New("Getting Server Instance error :" + err.Error())
		rpcMethod.rpcServer.logger.Println(customError)
		return customError
	}

	//get inputId from the []interface
	var newPredecessor chord.ServerInfoWithID
	//again marshal and unmarshal - reason it was getting marshalled into map[string]interface{}
	serverInfoBytes, _ := json.Marshal(jsonInput.Params[0])
	if err = json.Unmarshal(serverInfoBytes, &newPredecessor); err != nil {
		rpcMethod.rpcServer.logger.Println(err)
	}
	rpcMethod.rpcServer.logger.Println("In PredecessorLeft")
	rpcMethod.rpcServer.logger.Println(string(serverInfoBytes))
	//if newPredecessor.Id != own id
	if newPredecessor.Id != rpcMethod.rpcServer.chordNode.Id {
		rpcMethod.rpcServer.chordNode.SetPredecessor(false, newPredecessor.Id)
		rpcMethod.rpcServer.chordNode.FtServerMapping[newPredecessor.Id] = newPredecessor.ServerInfo
	} else {
		rpcMethod.rpcServer.chordNode.SetPredecessor(true, 0)

	}
	//response
	jsonOutput.Result = make([]interface{}, 1)
	jsonOutput.Result[0] = true

	return nil

}

/*
set new Successor when successor has left
request <-"{"method":"SuccesorLeft","params":[SuccessorServerinfo_object]}"
response <- "{"result":[true],"id":,"error":null }"
*/
func (rpcMethod *RPCMethod) SuccessorLeft(jsonInput RequestParameters, jsonOutput *ResponseParameters) error {
	//Initialize rpcserver
	var err error
	err, rpcMethod.rpcServer = GetRPCServerInstance()
	var customError error
	if err != nil {
		customError = errors.New("Getting Server Instance error :" + err.Error())
		rpcMethod.rpcServer.logger.Println(customError)
		return customError
	}

	//get inputId from the []interface
	var newSuccessor chord.ServerInfoWithID
	//again marshal and unmarshal - reason it was getting marshalled into map[string]interface{}
	serverInfoBytes, _ := json.Marshal(jsonInput.Params[0])
	if err = json.Unmarshal(serverInfoBytes, &newSuccessor); err != nil {
		rpcMethod.rpcServer.logger.Println(err)
	}
	rpcMethod.rpcServer.logger.Println("In SuccessorLeft")
	rpcMethod.rpcServer.logger.Println(string(serverInfoBytes))

	rpcMethod.rpcServer.chordNode.Successor = newSuccessor.Id
	rpcMethod.rpcServer.chordNode.FingerTable[1] = newSuccessor.Id
	rpcMethod.rpcServer.chordNode.FtServerMapping[newSuccessor.Id] = newSuccessor.ServerInfo

	//response
	jsonOutput.Result = make([]interface{}, 1)
	jsonOutput.Result[0] = true

	return nil

}

/*
Called in Shutdown - keys are transferred to successor
*/
func (rpcServer *RPCServer) makeInsertsToSuccessor() {

	//open a read transaction
	rpcServer.boltDB.Update(func(tx *bolt.Tx) error {
		var cursor *bolt.Cursor
		cursor = tx.Cursor()

		var bucket *bolt.Bucket

		//traverse through all keys
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			bucket = tx.Bucket(k)

			//traverse through all relation and value pairs
			bucket.ForEach(func(relation, value []byte) error {
				//create paramter - successor

				//add to array of interface

				parameterArray := make([]interface{}, 3)
				parameterArray[0] = string(k)
				parameterArray[1] = string(relation)
				if err := json.Unmarshal(value, &parameterArray[2]); err != nil {
					rpcServer.logger.Println(err)
					return nil

				}

				//create json message
				jsonMessage := rpcclient.RequestParameters{}
				jsonMessage.Method = "PureInsert"
				jsonMessage.Params = parameterArray

				var jsonBytes []byte
				jsonBytes, err := json.Marshal(jsonMessage)
				if err != nil {
					rpcServer.logger.Println(err)
					return err
				}

				rpcServer.logger.Println(string(jsonBytes))

				clientServerInfo, err := rpcServer.chordNode.PrepareClientServerInfo(rpcServer.chordNode.FingerTable[1])
				if err != nil {

					rpcServer.logger.Println(err)
					return nil

				}
				client := &rpcclient.RPCClient{}
				err, _ = client.RpcCall(clientServerInfo, string(jsonBytes))

				if err != nil {
					rpcServer.logger.Println(err)
					return nil
				}

				//delete the relation value pair
				if err = bucket.Delete(relation); err != nil {
					rpcServer.logger.Println(err)

				}

				return nil
			}) //end inner function
		} //end for
		return nil
	})

}

//called by lookup
func (rpcMethod *RPCMethod) checkIfPartialAndForwardRequest(jsonInput RequestParameters) (error, ResponseParameters) {

	var response ResponseParameters
	var parameters []interface{}
	parameters = jsonInput.Params
	var isPartialQuery bool
	isPartialQuery = false
	var err error
	//Use dict3 struct to unmarshall
	var key string
	var relation string
	for k, v := range parameters {

		if k == 0 {
			key = v.(string)
			if key == "" {
				isPartialQuery = true
			}
		} else if k == 1 {
			relation = v.(string)

			if relation == "" {
				isPartialQuery = true
			}
		}
	}

	//if it is a partial query call partialForwardRequestWrapper
	if isPartialQuery {
		//GetSuccessorInfoForInputHashWrapper(key,relation)
		// vvvvvvvvvvvvv  BEING WORKED BY SID vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv
		if key == "" { // if empty key
			//relationHash := hashing.GetStartingBits(relation,rpcMethod.rpcServer.chordNode.RelationHashLength)
			relationHash := hashing.GetEndingBits(relation, rpcMethod.rpcServer.chordNode.RelationHashLength)
			// get relation hash
			keyHash := uint32(0) // set first key hash to 0
			finalChordID := keyHash<<uint(rpcMethod.rpcServer.chordNode.RelationHashLength) | relationHash
			// concatenate
			var successorInfo chord.ServerInfoWithID
			successorInfo, err = rpcMethod.rpcServer.chordNode.GetSuccessorInfoForInputHash(finalChordID)
			if err != nil {
				rpcMethod.rpcServer.logger.Println(err)
				return err, ResponseParameters{}
			}

			firstSuccessorId := successorInfo.Id // make note of
			var msb uint32
			var lsb uint32

			for { // do forever
				//TODO			//forwardRequest to successorInfo 											// forward partial query
				msb = successorInfo.Id / uint32(rpcMethod.rpcServer.chordNode.RelationHashLength)
				// get most significant bits of chordNode Id, corresponding to key hash
				lsb = successorInfo.Id % (keyHash<<uint(rpcMethod.rpcServer.chordNode.RelationHashLength) | uint32(0))
				// get least significant bits of chordNode Id, corresponding to relation hash
				if lsb < relationHash {
					keyHash = msb // only have to set the least significant bits to relationHash for next iter
				} else {
					keyHash = msb + uint32(1) // if greater than or equal, have to increment mostSignificant
				}
				finalChordID := keyHash<<uint(rpcMethod.rpcServer.chordNode.RelationHashLength) | relationHash
				// concatenate
				successorInfo, err = rpcMethod.rpcServer.chordNode.GetSuccessorInfoForInputHash(finalChordID)
				if err != nil {
					rpcMethod.rpcServer.logger.Println(err)
					return err, ResponseParameters{}
				}
				if successorInfo.Id == firstSuccessorId {
					break
				}
			}
		} else { // empty relation
			keyHash := hashing.GetStartingBits(key, rpcMethod.rpcServer.chordNode.KeyHashLength)
			// get key hash
			relationHash := uint32(0) // set first relation hash to 0
			finalChordID := keyHash<<uint(rpcMethod.rpcServer.chordNode.RelationHashLength) | relationHash
			// concatenate
			var successorInfo chord.ServerInfoWithID
			successorInfo, err = rpcMethod.rpcServer.chordNode.GetSuccessorInfoForInputHash(finalChordID)
			if err != nil {
				rpcMethod.rpcServer.logger.Println(err)
				return err, ResponseParameters{}
			}

			firstSuccessorId := successorInfo.Id // make note of
			var msb uint32
			var lsb uint32

			for { // do forever
				//TODO			//forwardRequest to successorInfo 											// forward partial query
				msb = successorInfo.Id / uint32(rpcMethod.rpcServer.chordNode.RelationHashLength)
				// get most significant bits of chordNode Id, corresponding to key hash
				lsb = successorInfo.Id % (keyHash<<uint(rpcMethod.rpcServer.chordNode.RelationHashLength) | uint32(0))
				// get least significant bits of chordNode Id, corresponding to relation hash
				if msb != keyHash {
					break // if msb is not keyHash, all relevant values were retrieved from this node
				} else {
					relationHash = lsb + uint32(1) // else increment lsb to get next relevant hash
					//TODO												// what happens if lsb is 1111 ?
				}
				finalChordID := keyHash<<uint(rpcMethod.rpcServer.chordNode.RelationHashLength) | relationHash
				// concatenate
				successorInfo, err = rpcMethod.rpcServer.chordNode.GetSuccessorInfoForInputHash(finalChordID)
				if err != nil {
					rpcMethod.rpcServer.logger.Println(err)
					return err, ResponseParameters{}
				}
				if successorInfo.Id == firstSuccessorId {
					break // corner case
				}
			}
		}
		// ^^^^^^^^^^^^^  BEING WORKED BY SID ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
	} else {

		/**********************get Successor for the lnsert**********************/
		var parameters []interface{}
		parameters = jsonInput.Params

		//Use dict3 struct to unmarshall
		var key string
		var relation string
		for k, v := range parameters {

			if k == 0 {
				key = v.(string)
			} else if k == 1 {
				relation = v.(string)
			}
		}

		var finalChordID uint32
		finalChordID = rpcMethod.rpcServer.chordNode.GetHashFromKeyAndValue(key, relation)

		var successorInfo chord.ServerInfoWithID
		successorInfo, err = rpcMethod.rpcServer.chordNode.GetSuccessorInfoForInputHash(finalChordID)

		if err != nil {
			rpcMethod.rpcServer.logger.Println(err)
			return err, ResponseParameters{}
		}
		//for the target successor  - ID returned will be the same as chordNode.Id
		if successorInfo.Id != rpcMethod.rpcServer.chordNode.Id {

			jsonBytes, err := json.Marshal(jsonInput)
			if err != nil {
				rpcMethod.rpcServer.logger.Println(err)
				return err, ResponseParameters{}
			}

			var responseTemp interface{}
			client := &rpcclient.RPCClient{}
			err, responseTemp = client.RpcCall(successorInfo.ServerInfo, string(jsonBytes))

			if err != nil {
				rpcMethod.rpcServer.logger.Println(err)
				return nil, ResponseParameters{}
			}

			response.Result = responseTemp.(*rpcclient.ResponseParameters).Result
			response.Error = responseTemp.(*rpcclient.ResponseParameters).Error

			return nil, response
			/********************get Successor for the lookup - ends*********************/
		} else {

			//make the actual call on the target successor
			if err := rpcMethod.lookup(jsonInput.Params, &response); err != nil {
				rpcMethod.rpcServer.logger.Println(err)
				response.Result = nil
				response.Error = 1
			}

			return nil, response
		} //end else - if target successor

	} //end else not partial query

	return nil, ResponseParameters{}
}

/*
Transfer keys from current node to requesting node (new join of requestin node)
request <-"{"method":"TransferKeysAfterJoin","params":[RequesterServerinfo_object]}"
response <- "{"result":[true],"id":,"error":null }"
*/
func (rpcMethod *RPCMethod) TransferKeysAfterJoin(jsonInput RequestParameters, jsonOutput *ResponseParameters) error {
	rpcMethod.rpcServer.logger.Println("TransferKeysAfterJoin")

	//Initialize rpcserver
	var err error
	err, rpcMethod.rpcServer = GetRPCServerInstance()
	var customError error
	if err != nil {
		customError = errors.New("Getting Server Instance error :" + err.Error())
		rpcMethod.rpcServer.logger.Println(customError)
		return customError
	}

	//get inputId from the []interface
	var chordNodeToSend chord.ServerInfoWithID
	//again marshal and unmarshal - reason it was getting marshalled into map[string]interface{}
	serverInfoBytes, _ := json.Marshal(jsonInput.Params[0])
	if err = json.Unmarshal(serverInfoBytes, &chordNodeToSend); err != nil {
		rpcMethod.rpcServer.logger.Println(err)
	}

	rpcMethod.rpcServer.transferKeysToRequestingNode(chordNodeToSend)
	return nil
}

/*
Called in TransferKeysAfterJoin - successor transfers keys to requesting node
condition to transfer - key relation hash less than requesting node
*/

func (rpcServer *RPCServer) transferKeysToRequestingNode(chordNodeToSend chord.ServerInfoWithID) {
	//open a read transaction
	rpcServer.logger.Println("In transferKeysToRequestingNode")
	rpcServer.boltDB.Update(func(tx *bolt.Tx) error {
		var cursor *bolt.Cursor
		cursor = tx.Cursor()

		var bucket *bolt.Bucket

		var clientServerInfo rpcclient.ServerInfo
		clientServerInfo.ServerID = chordNodeToSend.ServerInfo.ServerID
		clientServerInfo.Protocol = chordNodeToSend.ServerInfo.Protocol
		clientServerInfo.IpAddress = chordNodeToSend.ServerInfo.IpAddress
		clientServerInfo.Port = chordNodeToSend.ServerInfo.Port

		//traverse through all keys
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			bucket = tx.Bucket(k)

			//traverse through all relation and value pairs
			bucket.ForEach(func(relation, value []byte) error {
				//create paramter - successor

				//add to array of interface

				parameterArray := make([]interface{}, 3)
				parameterArray[0] = string(k)
				parameterArray[1] = string(relation)
				if err := json.Unmarshal(value, &parameterArray[2]); err != nil {
					rpcServer.logger.Println(err)
					return nil
				}

				//if hash value less than predecessor value - then only insert
				keyRelationHash := rpcServer.chordNode.GetHashFromKeyAndValue(string(k), string(relation))
				rpcServer.logger.Println("The requesting node:")
				rpcServer.logger.Print(chordNodeToSend.Id)
				rpcServer.logger.Println("Key relation hash:")
				rpcServer.logger.Print(keyRelationHash)
				rpcServer.logger.Println("Current node id:")
				rpcServer.logger.Print(rpcServer.chordNode.Id)

				//keyRelationHash lies between chordNodeToSend and  chordNode.Id then skip
				if chordNodeToSend.Id < rpcServer.chordNode.Id {
					if keyRelationHash > chordNodeToSend.Id && keyRelationHash <= rpcServer.chordNode.Id {
						return nil
					}
				} else {
					if keyRelationHash <= chordNodeToSend.Id || keyRelationHash > rpcServer.chordNode.Id {
						return nil
					}

				}

				//create json message
				jsonMessage := rpcclient.RequestParameters{}
				jsonMessage.Method = "PureInsert"
				jsonMessage.Params = parameterArray

				jsonBytes, err := json.Marshal(jsonMessage)
				if err != nil {
					rpcServer.logger.Println(err)
					return err
				}

				rpcServer.logger.Println(string(jsonBytes))

				client := &rpcclient.RPCClient{}
				err, _ = client.RpcCall(clientServerInfo, string(jsonBytes))

				if err != nil {
					rpcServer.logger.Println(err)
					return nil
				}
				rpcServer.logger.Println("Deleting key:" + string(k) + " and relation:" + string(relation))
				//delete the relation value pair
				if err = bucket.Delete(relation); err != nil {
					rpcServer.logger.Println(err)

				}

				return nil
			})
		}
		return nil
	})

}

/*
delete triplets that have not been accessed for a certain period of time
request <-"{"method":"Purge","params":[]}"
response <- "{"result":[true],"id":,"error":null }"
*/
func (rpcMethod *RPCMethod) Purge(jsonInput RequestParameters, jsonOutput *ResponseParameters) error {

	rpcMethod.rpcServer.logger.Println("In Function Purge")

	//Initialize rpcserver
	var err error
	err, rpcMethod.rpcServer = GetRPCServerInstance()
	var customError error
	if err != nil {
		customError = errors.New("Getting Server Instance error :" + err.Error())
		rpcMethod.rpcServer.logger.Println(customError)
		return customError
	}

	rpcMethod.rpcServer.makeDeletes()

	//if successor set to itself return nil - dont propagate the call
	if rpcMethod.rpcServer.chordNode.FingerTable[1] == rpcMethod.rpcServer.chordNode.Id {
		return nil
	}

	//propogate to successor
	//give input the src id
	parameter := rpcMethod.rpcServer.chordNode.Id

	//create json message
	jsonMessage := rpcclient.RequestParameters{}
	jsonMessage.Method = "purgeWithSrcId"
	jsonMessage.Params = make([]interface{}, 1)
	jsonMessage.Params[0] = parameter
	jsonBytes, err := json.Marshal(jsonMessage)
	if err != nil {
		rpcMethod.rpcServer.chordNode.Logger.Println(err)
		return err
	}
	rpcMethod.rpcServer.logger.Println("In Purge")
	rpcMethod.rpcServer.logger.Println(string(jsonBytes))

	clientServerInfo, err := rpcMethod.rpcServer.chordNode.PrepareClientServerInfo(rpcMethod.rpcServer.chordNode.FingerTable[1])
	if err == nil {

		client := &rpcclient.RPCClient{}
		err, _ := client.RpcCall(clientServerInfo, string(jsonBytes))

		if err != nil {
			rpcMethod.rpcServer.logger.Println(err)
		}

	} else {
		rpcMethod.rpcServer.logger.Println(err)
	}

	//response
	jsonOutput.Result = make([]interface{}, 1)
	jsonOutput.Result[0] = true

	return nil
}

/*
delete triplets that have not been accessed for a certain period of time
request <-"{"method":"PurgeWithSrcId","params":[10]}"
response <- "{"result":[true],"id":,"error":null }"
*/
func (rpcMethod *RPCMethod) PurgeWithSrcId(jsonInput RequestParameters, jsonOutput *ResponseParameters) error {

	rpcMethod.rpcServer.logger.Println("In Function Purge")

	//Initialize rpcserver
	var err error
	err, rpcMethod.rpcServer = GetRPCServerInstance()
	var customError error
	if err != nil {
		customError = errors.New("Getting Server Instance error :" + err.Error())
		rpcMethod.rpcServer.logger.Println(customError)
		return customError
	}

	//stopping condition
	if uint32(jsonInput.Params[0].(float64)) == rpcMethod.rpcServer.chordNode.Id {
		return nil
	}

	//delete current node triplets using access time
	rpcMethod.rpcServer.makeDeletes()

	//if successor set to itself return nil
	if rpcMethod.rpcServer.chordNode.FingerTable[1] == rpcMethod.rpcServer.chordNode.Id {
		return nil
	}

	//propogate to successor the Purge and give input the src id
	parameter := uint32(jsonInput.Params[0].(float64))

	//create json message
	jsonMessage := rpcclient.RequestParameters{}
	jsonMessage.Method = "purgeWithSrcId"
	jsonMessage.Params = make([]interface{}, 1)
	jsonMessage.Params[0] = parameter
	jsonBytes, err := json.Marshal(jsonMessage)
	if err != nil {
		rpcMethod.rpcServer.logger.Println(err)
		return err
	}
	rpcMethod.rpcServer.logger.Println("In PurgeWithSrcId")
	rpcMethod.rpcServer.logger.Println(string(jsonBytes))

	clientServerInfo, err := rpcMethod.rpcServer.chordNode.PrepareClientServerInfo(rpcMethod.rpcServer.chordNode.FingerTable[1])
	if err == nil {

		client := &rpcclient.RPCClient{}
		err, _ := client.RpcCall(clientServerInfo, string(jsonBytes))

		if err != nil {
			rpcMethod.rpcServer.logger.Println(err)
		}

	} else {
		rpcMethod.rpcServer.logger.Println(err)
	}

	//response
	jsonOutput.Result = make([]interface{}, 1)
	jsonOutput.Result[0] = true

	return nil
}

/*
Called in Purge - triplets are deleted if difference between access time and current time  is more than user specified time
*/
func (rpcServer *RPCServer) makeDeletes() {

	//open a read transaction
	rpcServer.boltDB.Update(func(tx *bolt.Tx) error {
		var cursor *bolt.Cursor
		cursor = tx.Cursor()

		var bucket *bolt.Bucket

		//traverse through all keys
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			bucket = tx.Bucket(k)

			//traverse through all relation and value pairs
			bucket.ForEach(func(relation, value []byte) error {

				//unmarshal the value
				var valueWrapper ValueWrapper
				if err := json.Unmarshal(value, &valueWrapper); err != nil {
					rpcServer.logger.Println(err)
					return err
				}

				//check permission as well
				if valueWrapper.Permission == "R" {
					return nil
				}
				//if last accessed time less than user specified duration then skip delete
				if valueWrapper.Accessed != nil && time.Since(*(valueWrapper.Accessed)) < rpcServer.chordNode.DurationToDelete {
					return nil
				}
				//if  value hasnt been accessed yet dont delete
				if valueWrapper.Accessed == nil {
					return nil
				}
				//delete the relation value pair
				if err := bucket.Delete(relation); err != nil {
					rpcServer.logger.Println(err)

				}

				return nil
			}) //end inner function
		} //end for
		return nil
	})

	//delete bucket if empty
	rpcServer.boltDB.Update(func(tx *bolt.Tx) error {
		var cursor *bolt.Cursor
		cursor = tx.Cursor()

		var bucket *bolt.Bucket

		//traverse through all keys
		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			bucket = tx.Bucket(k)

			var bucketStats bolt.BucketStats
			bucketStats = bucket.Stats()
			//if bucket empty delete bucket
			if bucketStats.KeyN == 0 {
				tx.DeleteBucket([]byte(k))
			}
		}
		return nil
	})

}

/*****************************Chord related functions**************************************************/
