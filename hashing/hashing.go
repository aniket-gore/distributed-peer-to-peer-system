package hashing

import "crypto/sha1"
import "math"
/*
func main() {
	s := "sha1 this string"

	h := sha1.New()
	h.Write([]byte(s))

	bs := h.Sum(nil)

	fmt.Println(s)
	fmt.Println(bs)
	fmt.Println(getEndingBits(bs,10))
}
*/

//wrapper fuction
func GetEndingBits(inputString string,numberOfBits int) uint32{
	h := sha1.New()
	h.Write([]byte(inputString))

	bs := h.Sum(nil)
	return getEndingMBits(bs,numberOfBits)
}

//wrapper function
func GetStartingBits(inputString string,numberOfBits int) uint32{
	h := sha1.New()
	h.Write([]byte(inputString))

	bs := h.Sum(nil)
	return getStartingMBits(bs,numberOfBits)
}

/*
get last m bits and return an uint32
*/
func getEndingMBits(allHash []byte,numberOfBits int) uint32{
	var newHash uint32 
	newHash = 0
	var temp_uint uint32
	//assign 4 bytes
	if numberOfBits>=32{
		
		temp_uint = uint32(allHash[19])
		newHash |= temp_uint
		
		temp_uint = uint32(allHash[18])
		newHash |= temp_uint << 8
		
		temp_uint = uint32(allHash[17])
		newHash |= temp_uint << 16
		
		temp_uint = uint32(allHash[16])
		newHash |= temp_uint << 24
		
		return newHash
		
	}

	numberOfBytes := numberOfBits/8;
	remainingBits := numberOfBits%8;
	
	
	var index int
	index =0
	for ;index<numberOfBytes;index++{
		temp_uint = uint32(allHash[19-index])
		newHash |=temp_uint << uint(8*index)
	}

	
	//get remaining bits
	
	temp_uint = uint32(allHash[19-index])
	//fmt.Println("Byte to be partially hashed ", temp_uint)
	mask := uint32(math.Pow(2,float64(remainingBits)) - 1)
	//fmt.Println("Mask created ", mask)

	temp_uint &= mask
	
	newHash |= temp_uint << uint(8*index)

	return newHash

}

/*
get first m bits and return an uint32
*/
func getStartingMBits(allHash []byte,numberOfBits int) uint32{
	
	var newHash uint32 
	newHash = 0
	var temp_uint uint32
	//assign 4 bytes
	if numberOfBits>=32{
		
		temp_uint = uint32(allHash[0])
		newHash |= temp_uint
		
		temp_uint = uint32(allHash[1])
		newHash |= temp_uint << 8
		
		temp_uint = uint32(allHash[2])
		newHash |= temp_uint << 16
		
		temp_uint = uint32(allHash[3])
		newHash |= temp_uint << 24
		
		return newHash
		
	}
	numberOfBytes := numberOfBits/8;
	remainingBits := numberOfBits%8;
	
	
	var index int
	index =0
	for ;index<numberOfBytes;index++{
		temp_uint = uint32(allHash[index])
		newHash |=temp_uint << uint(8*index)
	}
	
	//get remaining bits
	
	temp_uint = uint32(allHash[index])
	//fmt.Println("Byte to be partially hashed ", temp_uint)
	mask := uint32(math.Pow(2,float64(remainingBits)) - 1)
	//fmt.Println("Mask created ", mask)

	temp_uint &= mask
	
	newHash |= temp_uint << uint(8*index)

	return newHash
	
}
