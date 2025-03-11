package tikv

import (
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/store/tikv/tikvrpc"
	"github.com/pingcap/errors"
)

func (c *RawKVClient) SAdd(key []byte, value [][]byte) error {
	if len(value) == 0 {
		return errors.New("empty value is not supported")
	}

	req := tikvrpc.NewRequest(tikvrpc.CmdSAdd, &kvrpcpb.SetAddRequest{
		Key:     key,
		Members: value,
	})
	resp, _, err := c.sendReq(key, req, false)
	if err != nil {
		return errors.Trace(err)
	}
	if resp.Resp == nil {
		return errors.Trace(ErrBodyMissing)
	}
	cmdResp := resp.Resp.(*kvrpcpb.SetAddResponse)
	if cmdResp.GetSuccess() != true {
		return errors.New("failed to add set members")
	}
	return nil
}

func (c *RawKVClient) SMembers(key []byte) ([][]byte, error) {
	if len(key) == 0 {
		return nil, errors.New("empty value is not supported")
	}

	req := tikvrpc.NewRequest(tikvrpc.CmdSMembers, &kvrpcpb.SetMembersRequest{
		Key: key,
	})
	resp, _, err := c.sendReq(key, req, false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if resp.Resp == nil {
		return nil, errors.Trace(ErrBodyMissing)
	}
	cmdResp := resp.Resp.(*kvrpcpb.SetMembersResponse)
	if cmdResp.GetRegionError() != nil {
		return nil, errors.New("failed to add set members")
	}
	return cmdResp.Members, nil
}
