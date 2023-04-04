package main

import (
	"encoding/hex"
	"fmt"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/storage/db"
	"github.com/urfave/cli/v2"
)

//yungojs
var exprotToMysqlCmd = &cli.Command{
	Name:  "export-mysql",
	Usage: "导出ticket到mysql",
	Subcommands: []*cli.Command{
		exprotTicketCmd,
		exprotPieceCmd,
	},
}

var exprotTicketCmd = &cli.Command{
	Name:  "ticket",
	Usage: "导出ticket到mysql",
	Action: func(cctx *cli.Context) error {
		ctx := lcli.ReqContext(cctx)
		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		miner, err := nodeApi.ActorAddress(ctx)
		if err != nil {
			return err
		}

		fullApi, clos, err := lcli.GetFullNodeAPI(cctx)
		if err != nil {
			return err
		}
		defer clos()
		ts, err := fullApi.ChainHead(ctx)
		if err != nil {
			return err
		}
		ms, err := fullApi.StateMinerActiveSectors(ctx, miner, ts.Key())
		if err != nil {
			return err
		}
		fs, err := fullApi.StateMinerFaults(ctx, miner, ts.Key())
		if err != nil {
			return err
		}
		ms2, err := fullApi.StateMinerSectors(ctx, miner, &fs, ts.Key())
		if err != nil {
			return err
		}
		ms = append(ms, ms2...)
		db.NewEngine()
		for _, v := range ms {
			fmt.Print(v.SectorNumber, ",")
			status, err := nodeApi.SectorsStatus(ctx, v.SectorNumber, false)
			if err != nil {
				log.Warn("获取失败 ：", v.SectorNumber, err.Error())
				continue
			}

			//sql := `insert into ` + db.TableTicket + `(miner_id,sector_id,ticket,ticket_h,proving,cid_commd,cid_commr,precommit,commit)value(?,?,?,?,1,?,?,?,?)`
			sql := `insert into ` + db.TableTicket + `(miner_id,sector_id,ticket,ticket_h,proving)value(?,?,?,?,1)`
			if _, err = db.DBengine.Exec(sql, miner.String(), status.SectorID.String(), hex.EncodeToString(status.Ticket.Value), status.Ticket.Epoch.String()); err != nil {
				log.Error("保存ticket失败：", err.Error())
			}
		}

		return nil
	},
}

var exprotPieceCmd = &cli.Command{
	Name:  "piece",
	Usage: "导出piece到mysql",
	Action: func(cctx *cli.Context) error {
		ctx := lcli.ReqContext(cctx)
		nodeApi, closer, err := lcli.GetStorageMinerAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		miner, err := nodeApi.ActorAddress(ctx)
		if err != nil {
			return err
		}

		fullApi, clos, err := lcli.GetFullNodeAPI(cctx)
		if err != nil {
			return err
		}
		defer clos()
		ts, err := fullApi.ChainHead(ctx)
		if err != nil {
			return err
		}
		ms, err := fullApi.StateMinerActiveSectors(ctx, miner, ts.Key())
		if err != nil {
			return err
		}
		fs, err := fullApi.StateMinerFaults(ctx, miner, ts.Key())
		if err != nil {
			return err
		}
		ms2, err := fullApi.StateMinerSectors(ctx, miner, &fs, ts.Key())
		if err != nil {
			return err
		}
		ms = append(ms, ms2...)
		db.NewEngine()
		for _, val := range ms {
			status, err := nodeApi.SectorsStatus(ctx, val.SectorNumber, false)
			if err != nil {
				log.Error(err)
				continue
			}
			for i, v := range status.Pieces {
				sql := `insert into ` + db.TablePiece + `(miner_id,sector_id,piece_cid,piece_size,deal_id)value(?,?,?,?,?)`
				var DealID uint64
				if len(val.DealIDs) > i {
					DealID = uint64(val.DealIDs[i])
				}
				if _, err = db.DBengine.Exec(sql, miner.String(), status.SectorID.String(), v.Piece.PieceCID.String(), uint64(v.Piece.Size), DealID); err != nil {
					log.Error("保存ticket失败：", err.Error(), v.Piece.PieceCID.String(), ",", status.SectorID)
				}
			}

		}

		return nil
	},
}
