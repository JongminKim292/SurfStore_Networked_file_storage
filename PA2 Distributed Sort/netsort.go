package main

import (
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"time"

	"gopkg.in/yaml.v2"
)

type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

func readServerConfigs(configPath string) ServerConfigs {
	f, err := ioutil.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	err = yaml.Unmarshal(f, &scs)

	return scs
}

func errorCheck(e error, msg string) {
	if e != nil {
		panic(e)
	}
}

func sortByte(original []byte) []byte {
	items := make(map[string][]byte)

	for i := 0; i <= len(original)-100; i += 100 {
		key := hex.EncodeToString(original[i : i+10])
		val := original[i+10 : i+100]
		items[key] = val
	}

	keys := make([]string, 0, len(items))
	for k := range items {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	sorted := make([]byte, 0)

	for _, k := range keys {
		data, err := hex.DecodeString(k)
		errorCheck(err, "string decode")
		key := make([]byte, 10)
		val := make([]byte, 90)
		key = data
		val = items[k]
		sorted = append(sorted, key...)
		sorted = append(sorted, val...)
	}
	return sorted
}

func listenForData(ch chan<- []byte, cType string, host string, port string, serverNum int) {
	fmt.Println("Starting " + cType + " server on connHost: " + host + ", connPort: " + port)
	listener, err := net.Listen(cType, host+":"+port)
	if err != nil {
		fmt.Println("Error listening: ", err.Error())
		os.Exit(1)
	}
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error connecting:", err.Error())
			return
		}
		fmt.Println("Client " + conn.RemoteAddr().String() + " connected.")
		go handleConnection(conn, ch, serverNum)
	}
}

func sendData(thisServerByte [][]byte, host string, port string, serverNum int) {
	fmt.Printf("Sending data for %d\n", serverNum)

	client, err := net.Dial("tcp", host+":"+port)
	errorCheck(err, "sending data (dial error)")
	defer client.Close()
	total := make([]byte, 0)
	for i := 0; i < len(thisServerByte); i++ {
		if thisServerByte[i][0] == 1 { // in case stream_complete is true
			continue
		}
		total = append(total, thisServerByte[i][1:101]...)
	}

	_, err = client.Write(total)
	errorCheck(err, "write data error")
	// fmt.Printf("here sent servuer %d length %d of data\n", serverNum, len(total))
	client.Close()

}

func handleConnection(conn net.Conn, ch chan<- []byte, serverNum int) {
	fmt.Println("handleConnection executed")
	buf := make([]byte, 4000)
	for {
		bytes, err := conn.Read(buf)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Conn::Read: err %v at server : %d\n", err, serverNum)
			os.Exit(1)
		}
		dataFromClient := buf[0:bytes]
		ch <- dataFromClient
	}
}

func itemPrint(itemByServer map[int][][]byte, numberOfServer int) {
	fmt.Println("#############################################")
	for i := 0; i < numberOfServer; i++ {
		fmt.Printf("Here Server : %d \n", i)
		for j := 0; j < len(itemByServer[i]); j++ {
			fmt.Println(itemByServer[i][j])
		}
		fmt.Printf("total : %d 's bytes \n", len(itemByServer[i]))
	}
	fmt.Println("############################################")
}

func consolidateServerData(currList []byte, ch <-chan []byte, numberOfServer int) []byte {
	numberOfCompleted := 0
	consolidated := make([]byte, 0)
	for numberOfServer != numberOfCompleted {

		data := <-ch // receive data from channel
		currList = append(currList, data...)
		consolidated = sortByte(currList)
		numberOfCompleted += 1
	}

	return consolidated
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	// What is my serverId
	serverId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v", err)
	}
	fmt.Println("My server Id:", serverId)

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])
	fmt.Println("Got the following server configs:", scs)

	/*
		Implement Distributed Sort
	*/

	inputFile, err := ioutil.ReadFile(os.Args[2])
	errorCheck(err, "input file open error")

	numberOfServer := len(scs.Servers)
	digit := 0
	temp := numberOfServer
	for temp > 1 {
		digit += 1
		temp /= 2
	}

	itemByServer := make(map[int][][]byte)

	for i := 0; i < len(inputFile); i += 100 {
		eachPair := []byte{0} // stream_complete
		eachPair = append(eachPair, inputFile[i:i+100]...)
		serverBit := eachPair[1]
		serverBit >>= (8 - digit)
		keyForServer := 0
		temp := 1
		for serverBit > 0 {
			if (serverBit & 1) == 1 {
				keyForServer += temp
			}
			temp *= 2
			serverBit >>= 1
		}
		itemByServer[keyForServer] = append(itemByServer[keyForServer], eachPair)
	}
	fmt.Println("#######################################")
	fmt.Println(len(itemByServer[serverId]))
	//listen on a socket, using extra channel

	ch := make(chan []byte)
	defer close(ch)
	go listenForData(ch, "tcp", scs.Servers[serverId].Host, scs.Servers[serverId].Port, serverId)

	// for i := 0; i < numberOfServer; i += 1 {
	// 	newItem := <- chas[i]
	// 	newItem = append(newItem, itemByServer[i][]...)
	// }

	time.Sleep(3 * time.Second)
	for i := 0; i < numberOfServer; i += 1 {
		if i != serverId {
			go sendData(itemByServer[i], scs.Servers[i].Host, scs.Servers[i].Port, i)
		}
		time.Sleep(1 * time.Second)
	}

	// make sure your data is sent and you received all the data frome other servers
	curr := make([]byte, 0)
	for i := 0; i < len(itemByServer[serverId]); i++ {
		curr = append(curr, itemByServer[serverId][i][1:101]...)
	}

	consolidated := consolidateServerData(curr, ch, numberOfServer)

	time.Sleep(20 * time.Second)

	err = ioutil.WriteFile(os.Args[3], consolidated, 0)
	errorCheck(err, "write error")
	time.Sleep(20 * time.Second)

	fmt.Printf("final output : %d", len(consolidated))
}
