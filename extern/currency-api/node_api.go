package currency_api

import (
	"context"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/types"
)

type NodeCalls interface {
	//yungolzj
	WalletBalance(context.Context, address.Address) (types.BigInt, error)                      //perm:read
	//StateMinerInfo(context.Context, address.Address, types.TipSetKey) (miner.MinerInfo, error) //perm:read
	StateMinerInfo(context.Context, address.Address, types.TipSetKey) (api.MinerInfo, error) //perm:read
}
