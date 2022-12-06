package surfstore

import (
	context "context"
	"fmt"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	val, ok := bs.BlockMap[blockHash.Hash]

	if !ok {
		return nil, fmt.Errorf("BlockHash %v is not found in the map", blockHash.Hash)
	} else {
		return val, nil
	}
}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	bs.BlockMap[GetBlockHashString(block.BlockData)] = block
	return &Success{Flag: true}, nil
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	hashes := make([]string, 0)
	for i := 0; i < len(blockHashesIn.Hashes); i++ {
		_, ok := bs.BlockMap[blockHashesIn.Hashes[i]]
		if !ok {
			return nil, fmt.Errorf("HasBlock in BlockStore.go err, at %v", blockHashesIn.Hashes[i])
		} else {
			hashes = append(hashes, blockHashesIn.Hashes[i])
		}
	}
	return &BlockHashes{Hashes: hashes}, nil

}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
