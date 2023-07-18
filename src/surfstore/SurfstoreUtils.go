package surfstore

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
)

func isSameBlock(a []string, b []string) bool {
	if len(a) != len(b) {
		return false
	} else {
		for i := 0; i < len(a); i += 1 {
			if a[i] != b[i] {
				return false
			}
		}
	}
	return true
}

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {

	// When a client syncs its local base directory with the cloud, a number of things must be done to properly complete the sync operation.

	// The client should first scan the base directory, and for each file, compute that file’s hash list. The client should then consult the local index file and compare the results, to see whether (1) there are now new files in the base directory that aren’t in the index file, or (2) files that are in the index file, but have changed since the last time the client was executed (i.e., the hash list is different).

	localIndex, err := LoadMetaFromMetaFile(client.BaseDir)
	if err != nil {
		fmt.Printf("local index loading err %v \n", err)
	}
	remoteIndex := make(map[string]*FileMetaData)
	client.GetFileInfoMap(&remoteIndex)

	files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		fmt.Printf("base dir file load err %v \n", err)
	}

	currFiles := make(map[string][]string)

	for _, file := range files {
		fileName := file.Name()
		fmt.Println(fileName)
		if fileName == DEFAULT_META_FILENAME || fileName == ".DS_Store" {
			continue
		}
		currFile, err := os.Open(ConcatPath(client.BaseDir, fileName))
		if err != nil {
			fmt.Printf("open current file err %v \n", err)
		}
		for {
			buf := make([]byte, client.BlockSize)
			l, err := currFile.Read(buf)
			if err == io.EOF {
				break
			} else if err != nil {
				fmt.Printf("read err %v \n", err)
				break
			}
			buf = buf[:l]
			hashString := GetBlockHashString(buf)
			currFiles[fileName] = append(currFiles[fileName], hashString)
		}
		prev, isUsed := localIndex[fileName]

		// fmt.Println("prev : ", localIndex[fileName].BlockHashList)
		// fmt.Println("curr : ", currFiles[fileName])
		if isUsed {
			if !isSameBlock(prev.BlockHashList, currFiles[fileName]) {
				localIndex[fileName].BlockHashList = currFiles[fileName]
				localIndex[fileName].Version += 1
			}
		} else {
			newMetaFile := FileMetaData{Filename: fileName, Version: 1, BlockHashList: currFiles[fileName]}
			localIndex[fileName] = &newMetaFile
		}
	}

	for fileName, localdata := range localIndex {
		if _, ok := currFiles[fileName]; !ok {
			if !isDeleted(localdata.BlockHashList) {
				localdata.Version += 1
				localdata.BlockHashList = []string{"0"}
			}
		}
	}

	for fileName, remotedata := range remoteIndex {
		localdata, isUsed := localIndex[fileName]
		if isUsed {
			if remotedata.Version > localdata.Version {
				download(client, remotedata, localdata)
			} else {
				if localdata.Version == remotedata.Version {
					if !isSameBlock(localdata.BlockHashList, remotedata.BlockHashList) {
						download(client, remotedata, localdata)
					}
				}
			}
		} else {
			localIndex[fileName] = &FileMetaData{}
			download(client, remotedata, localIndex[fileName])
		}
	}

	for fileName, localdata := range localIndex {
		remotedata, isUsed := remoteIndex[fileName]
		if isUsed {
			if remotedata.Version < localdata.Version {
				upload(client, localdata)
			}
		} else {
			upload(client, localdata)
		}
	}

	client.GetFileInfoMap(&remoteIndex)

	fmt.Println("#########")
	fmt.Println("local start")
	PrintMetaMap(localIndex)
	fmt.Println("#########")

	fmt.Println("#########")
	fmt.Println("remote start")
	PrintMetaMap(remoteIndex)
	fmt.Println("#########")

	WriteMetaFile(localIndex, client.BaseDir)

}

func download(client RPCClient, remoteMeta *FileMetaData, localMeta *FileMetaData) error {
	URL := ConcatPath(client.BaseDir, remoteMeta.Filename)
	file, err := os.Create(URL)
	if err != nil {
		fmt.Printf("create err %v \n", err)
	}
	defer file.Close()
	var blockAddr string
	if err := client.GetBlockStoreAddr(&blockAddr); err != nil {
		fmt.Printf("getting address err %v \n", err)
	}
	*localMeta = *remoteMeta

	if isDeleted(remoteMeta.BlockHashList) {
		if err := os.Remove(URL); err != nil {
			fmt.Printf("remove file error for deleted file %v \n", err)
			return err
		}
		return nil
	}
	result := ""
	for _, hash := range remoteMeta.BlockHashList {
		var block Block
		if err := client.GetBlock(hash, blockAddr, &block); err != nil {
			fmt.Printf("load block err %v \n", err)
		}
		result += string(block.BlockData)
	}
	file.WriteString(result)
	return nil
}

func isExistFile(fname string) bool {
	if _, err := os.Stat(fname); os.IsNotExist(err) {
		return false
	}
	return true
}

func isDeleted(hash []string) bool {
	if len(hash) == 1 {
		if hash[0] == "0" {
			return true
		}
	}
	return false
}

func upload(client RPCClient, metaData *FileMetaData) error {
	URL := ConcatPath(client.BaseDir, metaData.Filename)
	var latest int32
	fmt.Println("upload started", URL)

	var blockAddr string
	if err := client.GetBlockStoreAddr(&blockAddr); err != nil {
		fmt.Printf("getting address err %v \n", err)
	}

	file, err := os.Open(URL)
	if err != nil {
		fmt.Printf("upload(create) err %v \n", err)
	}
	defer file.Close()

	for {
		buf := make([]byte, client.BlockSize)
		l, err := file.Read(buf)
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Printf("read err %v \n", err)
			break
		}
		buf = buf[:l]
		block := Block{BlockData: buf, BlockSize: int32(l)}
		var success bool
		if err := client.PutBlock(&block, blockAddr, &success); err != nil {
			fmt.Printf("putBlock err %v \n", err)
		}
	}

	if err := client.UpdateFile(metaData, &latest); err != nil {
		fmt.Printf("update err %v \n", err)
		metaData.Version = -1
	}
	metaData.Version = latest
	return nil
}
