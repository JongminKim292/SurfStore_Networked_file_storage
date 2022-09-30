package main

import (
//    "bufio"
	"io/ioutil"
	"log"
	"os"
	// "fmt"
	"encoding/hex"
	"sort"
)

func errorCheck(e error, msg string){
	if e != nil {
		panic(e)
	}
}

func byteComparison(){

}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 3 {
		log.Fatalf("Usage: %v inputfile outputfile\n", os.Args[0])
	}

	log.Printf("Sorting %s to %s\n", os.Args[1], os.Args[2])

	file, err := ioutil.ReadFile(os.Args[1])
	errorCheck(err, "open error")
	log.Printf("Here Binary")

	items := make(map[string][]byte)

	for i := 0; i <= len(file)-100; i += 100 {
		key := hex.EncodeToString(file[i:i+10])
		val := file[i+10:i+100]
		items[key] = val 
	}

	keys := make([]string, 0, len(items))
	for k := range items {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	ans := make([]byte, 0)

	for _, k := range keys {
		data, err := hex.DecodeString(k)
		errorCheck(err ,"string decode")
		key := make([]byte, 10)
		val := make([]byte, 90)
		key = data
		val = items[k] 
		ans = append(ans, key...)
		ans = append(ans, val...)
	}

	err = ioutil.WriteFile(os.Args[2], ans, 0)
	errorCheck(err, "write error")
	

}