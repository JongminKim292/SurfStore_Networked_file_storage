package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strconv"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) InsertServer(addr string) {
	// panic("to do")
	c.ServerMap[c.Hash(addr)] = addr

}

func (c ConsistentHashRing) DeleteServer(addr string) {
	// panic("to do")
	delete(c.ServerMap, c.Hash(addr))
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	// Find the next largest key from ServerMap
	var keyList []string
	for hash, _ := range c.ServerMap {
		keyList = append(keyList, hash)
	}
	sort.Strings(keyList)

	for i := 0; i < len(keyList); i += 1 {
		if keyList[i] > blockId {
			return c.ServerMap[keyList[i]]
		}
	}

	return c.ServerMap[keyList[0]]
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func (c ConsistentHashRing) OutputMap(blockHashes []string) map[string]string {
	res := make(map[string]string)
	for i := 0; i < len(blockHashes); i++ {
		res["block"+strconv.Itoa(i)] = c.GetResponsibleServer(blockHashes[i])
	}
	return res
}

func NewConsistentHashRing(numServers int, downServer []int) *ConsistentHashRing {
	c := &ConsistentHashRing{
		ServerMap: make(map[string]string),
	}

	for i := 0; i < numServers; i++ {
		c.InsertServer("blockstore" + strconv.Itoa(i))
	}

	for i := 0; i < len(downServer); i++ {
		c.DeleteServer("blockstore" + strconv.Itoa(downServer[i]))
	}

	return c
}
