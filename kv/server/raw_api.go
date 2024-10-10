package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	resp := kvrpcpb.RawGetResponse{}
	r, err := server.storage.Reader(nil)
	if err != nil {
		resp.Error = err.Error()
		return &resp, err
	}

	defer r.Close()

	val, err := r.GetCF(req.Cf, req.Key)

	if err != nil {
		resp.Error = err.Error()
		return &resp, err
	}

	if val == nil {
		resp.NotFound = true
		return &resp, nil
	} else {
		resp.Value = val
	}

	return &resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	resp := kvrpcpb.RawPutResponse{}

	var batch []storage.Modify
	batch = append(batch, storage.Modify{Data: storage.Put{Key: req.Key, Value: req.Value, Cf: req.Cf}})
	err := server.storage.Write(nil, batch)
	if err != nil {
		resp.Error = err.Error()
		return &resp, err
	}

	return &resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	resp := kvrpcpb.RawDeleteResponse{}

	var batch []storage.Modify
	batch = append(batch, storage.Modify{Data: storage.Delete{Key: req.Key, Cf: req.Cf}})
	err := server.storage.Write(nil, batch)
	if err != nil {
		resp.Error = err.Error()
		return &resp, err
	}

	return &resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	resp := kvrpcpb.RawScanResponse{}
	r, err := server.storage.Reader(nil)
	if err != nil {
		resp.Error = err.Error()
		return &resp, nil
	}
	defer r.Close()

	it := r.IterCF(req.Cf)
	defer it.Close()
	it.Seek(req.StartKey)

	for i := 0; i < int(req.Limit) && it.Valid(); i++ {
		kv := kvrpcpb.KvPair{}
		kv.Key = it.Item().Key()
		val, err := it.Item().Value()
		if err != nil {
			kv.Value = nil
		} else {
			kv.Value = val
		}

		resp.Kvs = append(resp.Kvs, &kv)

		it.Next()
	}

	return &resp, nil
}
