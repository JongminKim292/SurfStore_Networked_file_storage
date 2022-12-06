package surfstore

import (
	context "context"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap    map[string]*FileMetaData
	BlockStoreAddr string
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	fileInfoMap := &FileInfoMap{FileInfoMap: m.FileMetaMap}
	return fileInfoMap, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	fmt.Println("update file")
	// fmt.Println("input version : ", fileMetaData.Version)
	// fmt.Println("in-server version : ", m.FileMetaMap[fileMetaData.Filename].Version)
	prevItem, inUse := m.FileMetaMap[fileMetaData.Filename]
	if !inUse {
		fmt.Println("it is not in used saved to map")
		m.FileMetaMap[fileMetaData.Filename] = fileMetaData
	} else {
		if fileMetaData.Version == prevItem.Version+1 {
			fmt.Println("it is in used saved to map")
			m.FileMetaMap[fileMetaData.Filename] = fileMetaData
		} else {
			fileMetaData.Version = -1
		}
	}
	fmt.Println(m.FileMetaMap[fileMetaData.Filename])
	return &Version{Version: fileMetaData.Version}, nil
}

func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
	blockStoreAddress := &BlockStoreAddr{Addr: m.BlockStoreAddr}
	return blockStoreAddress, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddr string) *MetaStore {
	return &MetaStore{
		FileMetaMap:    map[string]*FileMetaData{},
		BlockStoreAddr: blockStoreAddr,
	}
}
