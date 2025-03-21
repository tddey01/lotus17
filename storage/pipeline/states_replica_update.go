package sealing

import (
	"bytes"
	"context"
	"time"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/builtin"
	"github.com/filecoin-project/go-state-types/builtin/v9/miner"
	"github.com/filecoin-project/go-state-types/exitcode"
	"github.com/filecoin-project/go-statemachine"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/actors/policy"
	"github.com/filecoin-project/lotus/chain/types"
)

func (m *Sealing) handleReplicaUpdate(ctx statemachine.Context, sector SectorInfo) error {
	// if the sector ended up not having any deals, abort the upgrade
	if !sector.hasDeals() {
		return ctx.Send(SectorAbortUpgrade{xerrors.New("sector had no deals")})
	}

	if err := checkPieces(ctx.Context(), m.maddr, sector.SectorNumber, sector.Pieces, m.Api, true); err != nil { // Sanity check state
		return handleErrors(ctx, err, sector)
	}
	//yungojs
	out, err := m.sealer.ReplicaUpdate(sector.sealingCtx(ctx.Context(),DealSectorPriority), m.minerSector(sector.SectorType, sector.SectorNumber), sector.pieceInfos())
	if err != nil {
		return ctx.Send(SectorUpdateReplicaFailed{xerrors.Errorf("replica update failed: %w", err)})
	}
	return ctx.Send(SectorReplicaUpdate{
		Out: out,
	})
}

func (m *Sealing) handleProveReplicaUpdate(ctx statemachine.Context, sector SectorInfo) error {
	if sector.UpdateSealed == nil || sector.UpdateUnsealed == nil {
		return xerrors.Errorf("invalid sector %d with nil UpdateSealed or UpdateUnsealed output", sector.SectorNumber)
	}
	if sector.CommR == nil {
		return xerrors.Errorf("invalid sector %d with nil CommR", sector.SectorNumber)
	}
	// Abort upgrade for sectors that went faulty since being marked for upgrade
	ts, err := m.Api.ChainHead(ctx.Context())
	if err != nil {
		log.Errorf("handleProveReplicaUpdate: api error, not proceeding: %+v", err)
		return nil
	}
	active, err := m.sectorActive(ctx.Context(), ts.Key(), sector.SectorNumber)
	if err != nil {
		log.Errorf("sector active check: api error, not proceeding: %+v", err)
		return nil
	}
	if !active {
		log.Errorf("sector marked for upgrade %d no longer active, aborting upgrade", sector.SectorNumber)
		return ctx.Send(SectorAbortUpgrade{})
	}
	//yungojs
	vanillaProofs, err := m.sealer.ProveReplicaUpdate1(sector.sealingCtx(ctx.Context(),DealSectorPriority), m.minerSector(sector.SectorType, sector.SectorNumber), *sector.CommR, *sector.UpdateSealed, *sector.UpdateUnsealed)
	if err != nil {
		return ctx.Send(SectorProveReplicaUpdateFailed{xerrors.Errorf("prove replica update (1) failed: %w", err)})
	}

	if err := checkPieces(ctx.Context(), m.maddr, sector.SectorNumber, sector.Pieces, m.Api, true); err != nil { // Sanity check state
		return handleErrors(ctx, err, sector)
	}
	//yungojs
	proof, err := m.sealer.ProveReplicaUpdate2(sector.sealingCtx(ctx.Context(),DealSectorPriority), m.minerSector(sector.SectorType, sector.SectorNumber), *sector.CommR, *sector.UpdateSealed, *sector.UpdateUnsealed, vanillaProofs)
	if err != nil {
		return ctx.Send(SectorProveReplicaUpdateFailed{xerrors.Errorf("prove replica update (2) failed: %w", err)})

	}
	return ctx.Send(SectorProveReplicaUpdate{
		Proof: proof,
	})
}

func (m *Sealing) handleSubmitReplicaUpdate(ctx statemachine.Context, sector SectorInfo) error {

	ts, err := m.Api.ChainHead(ctx.Context())
	if err != nil {
		log.Errorf("handleSubmitReplicaUpdate: api error, not proceeding: %+v", err)
		return nil
	}

	if err := checkReplicaUpdate(ctx.Context(), m.maddr, sector, ts.Key(), m.Api); err != nil {
		return ctx.Send(SectorSubmitReplicaUpdateFailed{})
	}

	sl, err := m.Api.StateSectorPartition(ctx.Context(), m.maddr, sector.SectorNumber, ts.Key())
	if err != nil {
		log.Errorf("handleSubmitReplicaUpdate: api error, not proceeding: %+v", err)
		return nil
	}
	updateProof, err := sector.SectorType.RegisteredUpdateProof()
	if err != nil {
		log.Errorf("failed to get update proof type from seal proof: %+v", err)
		return ctx.Send(SectorSubmitReplicaUpdateFailed{})
	}
	enc := new(bytes.Buffer)
	params := &miner.ProveReplicaUpdatesParams{
		Updates: []miner.ReplicaUpdate{
			{
				SectorID:           sector.SectorNumber,
				Deadline:           sl.Deadline,
				Partition:          sl.Partition,
				NewSealedSectorCID: *sector.UpdateSealed,
				Deals:              sector.dealIDs(),
				UpdateProofType:    updateProof,
				ReplicaProof:       sector.ReplicaUpdateProof,
			},
		},
	}
	if err := params.MarshalCBOR(enc); err != nil {
		log.Errorf("failed to serialize update replica params: %w", err)
		return ctx.Send(SectorSubmitReplicaUpdateFailed{})
	}

	cfg, err := m.getConfig()
	if err != nil {
		return xerrors.Errorf("getting config: %w", err)
	}

	onChainInfo, err := m.Api.StateSectorGetInfo(ctx.Context(), m.maddr, sector.SectorNumber, ts.Key())
	if err != nil {
		log.Errorf("handleSubmitReplicaUpdate: api error, not proceeding: %+v", err)
		return nil
	}
	sp, err := m.currentSealProof(ctx.Context())
	if err != nil {
		log.Errorf("sealer failed to return current seal proof not proceeding: %+v", err)
		return nil
	}
	virtualPCI := miner.SectorPreCommitInfo{
		SealProof:    sp,
		SectorNumber: sector.SectorNumber,
		SealedCID:    *sector.UpdateSealed,
		//SealRandEpoch: 0,
		DealIDs:    sector.dealIDs(),
		Expiration: onChainInfo.Expiration,
		//ReplaceCapacity: false,
		//ReplaceSectorDeadline: 0,
		//ReplaceSectorPartition: 0,
		//ReplaceSectorNumber: 0,
	}

	collateral, err := m.Api.StateMinerInitialPledgeCollateral(ctx.Context(), m.maddr, virtualPCI, ts.Key())
	if err != nil {
		return xerrors.Errorf("getting initial pledge collateral: %w", err)
	}

	collateral = big.Sub(collateral, onChainInfo.InitialPledge)
	if collateral.LessThan(big.Zero()) {
		collateral = big.Zero()
	}

	collateral, err = collateralSendAmount(ctx.Context(), m.Api, m.maddr, cfg, collateral)
	if err != nil {
		log.Errorf("collateral send amount failed not proceeding: %+v", err)
		return nil
	}

	goodFunds := big.Add(collateral, big.Int(m.feeCfg.MaxCommitGasFee))

	mi, err := m.Api.StateMinerInfo(ctx.Context(), m.maddr, ts.Key())
	if err != nil {
		log.Errorf("handleSubmitReplicaUpdate: api error, not proceeding: %+v", err)
		return nil
	}

	from, _, err := m.addrSel.AddressFor(ctx.Context(), m.Api, mi, api.CommitAddr, goodFunds, collateral)
	if err != nil {
		log.Errorf("no good address to send replica update message from: %+v", err)
		return ctx.Send(SectorSubmitReplicaUpdateFailed{})
	}
	mcid, err := sendMsg(ctx.Context(), m.Api, from, m.maddr, builtin.MethodsMiner.ProveReplicaUpdates, collateral, big.Int(m.feeCfg.MaxCommitGasFee), enc.Bytes())
	if err != nil {
		log.Errorf("handleSubmitReplicaUpdate: error sending message: %+v", err)
		return ctx.Send(SectorSubmitReplicaUpdateFailed{})
	}

	return ctx.Send(SectorReplicaUpdateSubmitted{Message: mcid})
}

func (m *Sealing) handleReplicaUpdateWait(ctx statemachine.Context, sector SectorInfo) error {
	if sector.ReplicaUpdateMessage == nil {
		log.Errorf("handleReplicaUpdateWait: no replica update message cid recorded")
		return ctx.Send(SectorSubmitReplicaUpdateFailed{})
	}

	mw, err := m.Api.StateWaitMsg(ctx.Context(), *sector.ReplicaUpdateMessage, build.MessageConfidence, api.LookbackNoLimit, true)
	if err != nil {
		log.Errorf("handleReplicaUpdateWait: failed to wait for message: %+v", err)
		return ctx.Send(SectorSubmitReplicaUpdateFailed{})
	}

	switch mw.Receipt.ExitCode {
	case exitcode.Ok:
		//expected
	case exitcode.SysErrInsufficientFunds:
		fallthrough
	case exitcode.SysErrOutOfGas:
		log.Errorf("gas estimator was wrong or out of funds")
		return ctx.Send(SectorSubmitReplicaUpdateFailed{})
	default:
		return ctx.Send(SectorSubmitReplicaUpdateFailed{})
	}
	si, err := m.Api.StateSectorGetInfo(ctx.Context(), m.maddr, sector.SectorNumber, mw.TipSet)
	if err != nil {
		log.Errorf("api err failed to get sector info: %+v", err)
		return ctx.Send(SectorSubmitReplicaUpdateFailed{})
	}
	if si == nil {
		log.Errorf("api err sector not found")
		return ctx.Send(SectorSubmitReplicaUpdateFailed{})
	}

	if !si.SealedCID.Equals(*sector.UpdateSealed) {
		return ctx.Send(SectorAbortUpgrade{xerrors.Errorf("mismatch of expected onchain sealed cid after replica update, expected %s got %s", sector.UpdateSealed, si.SealedCID)})
	}
	return ctx.Send(SectorReplicaUpdateLanded{})
}

func (m *Sealing) handleFinalizeReplicaUpdate(ctx statemachine.Context, sector SectorInfo) error {
	cfg, err := m.getConfig()
	if err != nil {
		return xerrors.Errorf("getting sealing config: %w", err)
	}
	//yungojs
	if err := m.sealer.FinalizeReplicaUpdate(sector.sealingCtx(ctx.Context(), DealSectorPriority), m.minerSector(sector.SectorType, sector.SectorNumber), sector.keepUnsealedRanges(sector.Pieces, false, cfg.AlwaysKeepUnsealedCopy)); err != nil {
		return ctx.Send(SectorFinalizeFailed{xerrors.Errorf("finalize sector: %w", err)})
	}

	return ctx.Send(SectorFinalized{})
}

func (m *Sealing) handleUpdateActivating(ctx statemachine.Context, sector SectorInfo) error {
	if sector.ReplicaUpdateMessage == nil {
		return xerrors.Errorf("nil sector.ReplicaUpdateMessage!")
	}

	try := func() error {
		mw, err := m.Api.StateWaitMsg(ctx.Context(), *sector.ReplicaUpdateMessage, build.MessageConfidence, api.LookbackNoLimit, true)
		if err != nil {
			return err
		}

		ts, err := m.Api.ChainHead(ctx.Context())
		if err != nil {
			return err
		}

		nv, err := m.Api.StateNetworkVersion(ctx.Context(), ts.Key())
		if err != nil {
			return err
		}

		lb := policy.GetWinningPoStSectorSetLookback(nv)

		targetHeight := mw.Height + lb + InteractivePoRepConfidence

		return m.events.ChainAt(context.Background(), func(context.Context, *types.TipSet, abi.ChainEpoch) error {
			return ctx.Send(SectorUpdateActive{})
		}, func(ctx context.Context, ts *types.TipSet) error {
			log.Warn("revert in handleUpdateActivating")
			return nil
		}, InteractivePoRepConfidence, targetHeight)
	}

	for {
		err := try()
		if err == nil {
			break
		}

		log.Errorw("error in handleUpdateActivating", "error", err)

		// likely an API issue, sleep for a bit and retry
		time.Sleep(time.Minute)
	}

	return nil
}

func (m *Sealing) handleReleaseSectorKey(ctx statemachine.Context, sector SectorInfo) error {
	//yungojs
	if err := m.sealer.ReleaseSectorKey(sector.sealingCtx(ctx.Context(), DealSectorPriority), m.minerSector(sector.SectorType, sector.SectorNumber)); err != nil {
		return ctx.Send(SectorReleaseKeyFailed{err})
	}

	return ctx.Send(SectorKeyReleased{})
}

func handleErrors(ctx statemachine.Context, err error, sector SectorInfo) error {
	switch err.(type) {
	case *ErrApi:
		log.Errorf("handleReplicaUpdate: api error, not proceeding: %+v", err)
		return nil
	case *ErrInvalidDeals:
		log.Warnf("invalid deals in sector %d: %v", sector.SectorNumber, err)
		return ctx.Send(SectorInvalidDealIDs{})
	case *ErrExpiredDeals: // Probably not much we can do here, maybe re-pack the sector?
		return ctx.Send(SectorDealsExpired{xerrors.Errorf("expired dealIDs in sector: %w", err)})
	default:
		return xerrors.Errorf("checkPieces sanity check error: %w (%+v)", err, err)
	}
}
