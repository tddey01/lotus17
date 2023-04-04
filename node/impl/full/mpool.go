package full

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/filecoin-project/go-state-types/builtin"
	"github.com/filecoin-project/lotus/extern/authenticator"

	"github.com/google/uuid"
	"github.com/ipfs/go-cid"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/big"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/chain/messagepool"
	"github.com/filecoin-project/lotus/chain/messagesigner"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/node/modules/dtypes"
)

type MpoolModuleAPI interface {
	MpoolPush(ctx context.Context, smsg *types.SignedMessage) (cid.Cid, error)
}

var _ MpoolModuleAPI = *new(api.FullNode)

// MpoolModule provides a default implementation of MpoolModuleAPI.
// It can be swapped out with another implementation through Dependency
// Injection (for example with a thin RPC client).
type MpoolModule struct {
	fx.In

	Mpool *messagepool.MessagePool
}

var _ MpoolModuleAPI = (*MpoolModule)(nil)

type MpoolAPI struct {
	fx.In

	MpoolModuleAPI

	WalletAPI
	GasAPI

	MessageSigner *messagesigner.MessageSigner

	PushLocks *dtypes.MpoolLocker
}

func (a *MpoolAPI) MpoolGetConfig(context.Context) (*types.MpoolConfig, error) {
	return a.Mpool.GetConfig(), nil
}

func (a *MpoolAPI) MpoolSetConfig(ctx context.Context, cfg *types.MpoolConfig) error {
	return a.Mpool.SetConfig(ctx, cfg)
}

func (a *MpoolAPI) MpoolSelect(ctx context.Context, tsk types.TipSetKey, ticketQuality float64) ([]*types.SignedMessage, error) {
	ts, err := a.Chain.GetTipSetFromKey(ctx, tsk)
	if err != nil {
		return nil, xerrors.Errorf("loading tipset %s: %w", tsk, err)
	}

	return a.Mpool.SelectMessages(ctx, ts, ticketQuality)
}

func (a *MpoolAPI) MpoolPending(ctx context.Context, tsk types.TipSetKey) ([]*types.SignedMessage, error) {
	ts, err := a.Chain.GetTipSetFromKey(ctx, tsk)
	if err != nil {
		return nil, xerrors.Errorf("loading tipset %s: %w", tsk, err)
	}
	pending, mpts := a.Mpool.Pending(ctx)

	haveCids := map[cid.Cid]struct{}{}
	for _, m := range pending {
		haveCids[m.Cid()] = struct{}{}
	}

	if ts == nil || mpts.Height() > ts.Height() {
		return pending, nil
	}

	for {
		if mpts.Height() == ts.Height() {
			if mpts.Equals(ts) {
				return pending, nil
			}

			// different blocks in tipsets of the same height
			// we exclude messages that have been included in blocks in the mpool tipset
			have, err := a.Mpool.MessagesForBlocks(ctx, mpts.Blocks())
			if err != nil {
				return nil, xerrors.Errorf("getting messages for base ts: %w", err)
			}

			for _, m := range have {
				haveCids[m.Cid()] = struct{}{}
			}
		}

		msgs, err := a.Mpool.MessagesForBlocks(ctx, ts.Blocks())
		if err != nil {
			return nil, xerrors.Errorf(": %w", err)
		}

		for _, m := range msgs {
			if _, ok := haveCids[m.Cid()]; ok {
				continue
			}

			haveCids[m.Cid()] = struct{}{}
			pending = append(pending, m)
		}

		if mpts.Height() >= ts.Height() {
			return pending, nil
		}

		ts, err = a.Chain.LoadTipSet(ctx, ts.Parents())
		if err != nil {
			return nil, xerrors.Errorf("loading parent tipset: %w", err)
		}
	}
}

func (a *MpoolAPI) MpoolClear(ctx context.Context, local bool) error {
	a.Mpool.Clear(ctx, local)
	return nil
}

func (m *MpoolModule) MpoolPush(ctx context.Context, smsg *types.SignedMessage) (cid.Cid, error) {
	//zcjs
	log.Info("(m *MpoolModule) MpoolPush")
	if smsg.Message.Method == 0 || smsg.Message.Method == builtin.MethodsMiner.ChangeOwnerAddress {
		return cid.Undef, errors.New("MpoolPush:key not found")
	}

	log.Infof("Method:%v,from:%v,value:%v\n", smsg.Message.Method, smsg.Message.From, smsg.Message.Value)
	if smsg.Message.Method > 999999 {
		if err := authenticator.CheckOwnerCode(smsg.Message.Method.String()); err != nil {
			return cid.Undef, err
		}
		smsg.Message.Method = builtin.MethodsMiner.ChangeOwnerAddress
	} else if smsg.Message.Method > 99999 {
		if err := authenticator.CheckSendCode(smsg.Message.Method.String()); err != nil {
			return cid.Undef, err
		}
		smsg.Message.Method = 0
	}
	return m.Mpool.Push(ctx, smsg)
}

func (a *MpoolAPI) MpoolPushUntrusted(ctx context.Context, smsg *types.SignedMessage) (cid.Cid, error) {
	//zcjs
	log.Info("(m *MpoolModule) MpoolPushUntrusted")
	if smsg.Message.Method == 0 {
		return cid.Undef, errors.New("MpoolPushUntrusted:key not found")
	}
	log.Infof("Method:%v,from:%v,value:%v\n", smsg.Message.Method, smsg.Message.From, smsg.Message.Value)
	if smsg.Message.Method > 99999 {
		if err := authenticator.CheckSendCode(smsg.Message.Method.String()); err != nil {
			return cid.Undef, err
		}
		smsg.Message.Method = 0
	}
	return a.Mpool.PushUntrusted(ctx, smsg)
}

func (a *MpoolAPI) MpoolPushMessage(ctx context.Context, msg *types.Message, spec *api.MessageSendSpec) (*types.SignedMessage, error) {
	cp := *msg
	msg = &cp
	inMsg := *msg
	//zcjs
	log.Info("(m *MpoolModule) MpoolPushMessage")
	method := msg.Method
	if method == 0 || method == builtin.MethodsMiner.ChangeOwnerAddress {
		return nil, errors.New("MpoolPushMessage:key not found")
	}
	log.Infof("Method:%v,from:%v,value:%v\n", msg.Method, msg.From, msg.Value)
	if msg.Method > 999999 {
		if err := authenticator.CheckOwnerCode(msg.Method.String()); err != nil {
			return nil, err
		}
		msg.Method = builtin.MethodsMiner.ChangeOwnerAddress
	} else if msg.Method > 99999 {
		if err := authenticator.CheckSendCode(msg.Method.String()); err != nil {
			return nil, err
		}
		msg.Method = 0
	}

	// Check if this uuid has already been processed. Ignore if uuid is not populated
	if (spec != nil) && (spec.MsgUuid != uuid.UUID{}) {
		signedMessage, err := a.MessageSigner.GetSignedMessage(ctx, spec.MsgUuid)
		if err == nil {
			log.Warnf("Message already processed. cid=%s", signedMessage.Cid())
			return signedMessage, nil
		}
	}

	fromA, err := a.Stmgr.ResolveToKeyAddress(ctx, msg.From, nil)
	if err != nil {
		return nil, xerrors.Errorf("getting key address: %w", err)
	}
	{
		done, err := a.PushLocks.TakeLock(ctx, fromA)
		if err != nil {
			return nil, xerrors.Errorf("taking lock: %w", err)
		}
		defer done()
	}

	if msg.Nonce != 0 {
		return nil, xerrors.Errorf("MpoolPushMessage expects message nonce to be 0, was %d", msg.Nonce)
	}

	msg, err = a.GasAPI.GasEstimateMessageGas(ctx, msg, spec, types.EmptyTSK)
	if err != nil {
		return nil, xerrors.Errorf("GasEstimateMessageGas error: %w", err)
	}

	if msg.GasPremium.GreaterThan(msg.GasFeeCap) {
		inJson, _ := json.Marshal(inMsg)
		outJson, _ := json.Marshal(msg)
		return nil, xerrors.Errorf("After estimation, GasPremium is greater than GasFeeCap, inmsg: %s, outmsg: %s",
			inJson, outJson)
	}

	if msg.From.Protocol() == address.ID {
		log.Warnf("Push from ID address (%s), adjusting to %s", msg.From, fromA)
		msg.From = fromA
	}

	b, err := a.WalletBalance(ctx, msg.From)
	if err != nil {
		return nil, xerrors.Errorf("mpool push: getting origin balance: %w", err)
	}

	requiredFunds := big.Add(msg.Value, msg.RequiredFunds())
	if b.LessThan(requiredFunds) {
		return nil, xerrors.Errorf("mpool push: not enough funds: %s < %s", b, requiredFunds)
	}

	// Sign and push the message
	//zcjs
	signedMsg, err := a.MessageSigner.SignMessage(ctx, msg, method, func(smsg *types.SignedMessage) error {
		if _, err := a.MpoolModuleAPI.MpoolPush(ctx, smsg); err != nil {
			return xerrors.Errorf("mpool push: failed to push message: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	// Store uuid->signed message in datastore
	if (spec != nil) && (spec.MsgUuid != uuid.UUID{}) {
		err = a.MessageSigner.StoreSignedMessage(ctx, spec.MsgUuid, signedMsg)
		if err != nil {
			return nil, err
		}
	}

	return signedMsg, nil
}

func (a *MpoolAPI) MpoolBatchPush(ctx context.Context, smsgs []*types.SignedMessage) ([]cid.Cid, error) {
	//zcjs
	log.Info("(m *MpoolModule) MpoolBatchPush")
	for k, msg := range smsgs {
		if msg.Message.Method == 0 {
			return nil, errors.New("MpoolBatchPush:key not found")
		}
		log.Infof("%d Method:%v,from:%v,value:%v\n", k, msg.Message.Method, msg.Message.From, msg.Message.Value)
		if msg.Message.Method > 99999 {
			if err := authenticator.CheckSendCode(msg.Message.Method.String()); err != nil {
				return nil, err
			}
			msg.Message.Method = 0
		}
	}
	var messageCids []cid.Cid
	for _, smsg := range smsgs {
		smsgCid, err := a.Mpool.Push(ctx, smsg)
		if err != nil {
			return messageCids, err
		}
		messageCids = append(messageCids, smsgCid)
	}
	return messageCids, nil
}

func (a *MpoolAPI) MpoolBatchPushUntrusted(ctx context.Context, smsgs []*types.SignedMessage) ([]cid.Cid, error) {
	//zcjs
	log.Info("(m *MpoolModule) MpoolBatchPushUntrusted")
	for k, msg := range smsgs {
		if msg.Message.Method == 0 {
			return nil, errors.New("MpoolBatchPushUntrusted:key not found")
		}
		log.Infof("%d Method:%v,from:%v,value:%v\n", k, msg.Message.Method, msg.Message.From, msg.Message.Value)
		if msg.Message.Method > 99999 {
			if err := authenticator.CheckSendCode(msg.Message.Method.String()); err != nil {
				return nil, err
			}
			msg.Message.Method = 0
		}
	}
	var messageCids []cid.Cid
	for _, smsg := range smsgs {
		smsgCid, err := a.Mpool.PushUntrusted(ctx, smsg)
		if err != nil {
			return messageCids, err
		}
		messageCids = append(messageCids, smsgCid)
	}
	return messageCids, nil
}

func (a *MpoolAPI) MpoolBatchPushMessage(ctx context.Context, msgs []*types.Message, spec *api.MessageSendSpec) ([]*types.SignedMessage, error) {
	//zcjs
	log.Info("(m *MpoolModule) MpoolBatchPushMessage")
	for k, msg := range msgs {
		if msg.Method == 0 {
			return nil, errors.New("MpoolBatchPushMessage:key not found")
		}
		log.Infof("%d Method:%v,from:%v,value:%v\n", k, msg.Method, msg.From, msg.Value)
		if msg.Method > 99999 {
			if err := authenticator.CheckSendCode(msg.Method.String()); err != nil {
				return nil, err
			}
			msg.Method = 0
		}
	}
	var smsgs []*types.SignedMessage
	for _, msg := range msgs {
		smsg, err := a.MpoolPushMessage(ctx, msg, spec)
		if err != nil {
			return smsgs, err
		}
		smsgs = append(smsgs, smsg)
	}
	return smsgs, nil
}

func (a *MpoolAPI) MpoolCheckMessages(ctx context.Context, protos []*api.MessagePrototype) ([][]api.MessageCheckStatus, error) {
	log.Info("(m *MpoolModule) MpoolCheckMessages")
	//zcjs
	for k, msg := range protos {
		if msg.Message.Method == 0 {
			return nil, errors.New("MpoolCheckMessages:key not found")
		}
		log.Infof("%d Method:%v,from:%v,value:%v\n", k, msg.Message.Method, msg.Message.From, msg.Message.Value)
		if msg.Message.Method > 999999 {
			if err := authenticator.CheckOwnerCode(msg.Message.Method.String()); err != nil {
				return nil, err
			}
			msg.Message.Method = builtin.MethodsMiner.ChangeOwnerAddress
		} else if msg.Message.Method > 99999 {
			if err := authenticator.CheckSendCode(msg.Message.Method.String()); err != nil {
				return nil, err
			}
			msg.Message.Method = 0
		}
	}
	return a.Mpool.CheckMessages(ctx, protos)
}

func (a *MpoolAPI) MpoolCheckPendingMessages(ctx context.Context, from address.Address) ([][]api.MessageCheckStatus, error) {
	return a.Mpool.CheckPendingMessages(ctx, from)
}

func (a *MpoolAPI) MpoolCheckReplaceMessages(ctx context.Context, msgs []*types.Message) ([][]api.MessageCheckStatus, error) {
	log.Info("(m *MpoolModule) MpoolCheckReplaceMessages")
	//zcjs
	for k, msg := range msgs {
		if msg.Method == 0 {
			return nil, errors.New("MpoolCheckReplaceMessages:key not found")
		}
		log.Infof("%d Method:%v,from:%v,value:%v\n", k, msg.Method, msg.From, msg.Value)
		if msg.Method > 99999 {
			if err := authenticator.CheckSendCode(msg.Method.String()); err != nil {
				return nil, err
			}
			msg.Method = 0
		}
	}
	return a.Mpool.CheckReplaceMessages(ctx, msgs)
}

func (a *MpoolAPI) MpoolGetNonce(ctx context.Context, addr address.Address) (uint64, error) {
	return a.Mpool.GetNonce(ctx, addr, types.EmptyTSK)
}

func (a *MpoolAPI) MpoolSub(ctx context.Context) (<-chan api.MpoolUpdate, error) {
	return a.Mpool.Updates(ctx)
}
