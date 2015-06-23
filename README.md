A distributed system to perform dictionary operations of store/add/delete/modify/lookup and partial lookup using peer-to-peer CHORD protocol.

Features:
- CHORD protocol
- scalable
- persistent storage
- assumes unstable network
- out-of-order execution
- error-handling


Configuration instructions:

make server
Installs the user server packages and third party library “BOLT”. Uses $GOPATH for
installation.

make clean
Cleans the installed packages and the third party library. Also cleans the executable created. Uses
$GOBIN.

Run Server:
testserver server_config_file
