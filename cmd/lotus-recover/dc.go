package main

import (
	"context"
	"fmt"
	"github.com/docker/go-units"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-padreader"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/storage/db"
	"github.com/filecoin-project/lotus/storage/sealer/ffiwrapper"
	"github.com/filecoin-project/lotus/storage/sealer/ffiwrapper/basicfs"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
	"os"
	"path"
	"strconv"
	"time"
)

var sealDcCmd = &cli.Command{
	Name:  "dc",
	Usage: "Benchmark seal and winning post and window post",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "storage-dir",
			Value: ".plus",
			Usage: "path to the storage directory that will store sectors long term",
		},
		&cli.StringFlag{
			Name:  "datapath",
			Value: "",
			Usage: "原值扇区文件目录",
		},
		&cli.StringFlag{
			Name:  "sector-size",
			Value: "32GiB",
			Usage: "size of the sectors in bytes, i.e. 32GiB",
		},
		&cli.BoolFlag{
			Name:  "no-gpu",
			Usage: "disable gpu usage for the benchmark run",
		},
		&cli.StringFlag{
			Name:  "miner-addr",
			Usage: "pass miner address (only necessary if using existing sectorbuilder)",
		},
		&cli.IntFlag{
			Name:  "parallel",
			Usage: "num run in parallel",
			Value: 2,
		},
	},
	Action: func(c *cli.Context) error {
		if c.Bool("no-gpu") {
			err := os.Setenv("BELLMAN_NO_GPU", "1")
			if err != nil {
				return xerrors.Errorf("setting no-gpu flag: %w", err)
			}
		}
		datadir := c.String("datapath")
		if c.String("miner-addr") == "" || c.String("datapath") == "" {
			return xerrors.Errorf("原值数据不能为空，矿工号不能为空！")
		}

		sdir, err := homedir.Expand(c.String("storage-dir"))
		if err != nil {
			return err
		}

		err = os.MkdirAll(sdir, 0775) //nolint:gosec
		if err != nil {
			return xerrors.Errorf("creating sectorbuilder dir: %w", err)
		}
		sbfs := &basicfs.Provider{
			Root: sdir,
		}

		sb, err := ffiwrapper.New(sbfs)
		if err != nil {
			return err
		}
		maddr, err := address.NewFromString(c.String("miner-addr"))
		if err != nil {
			return err
		}
		amid, err := address.IDFromAddress(maddr)
		if err != nil {
			return err
		}
		sectorSizeInt, err := units.RAMInBytes(c.String("sector-size"))
		if err != nil {
			return err
		}

		return sealDcSector(c.Context, sb, sdir, datadir, c.Int("parallel"), abi.ActorID(amid), abi.SectorSize(sectorSizeInt))
	},
}

func sealDcSector(c context.Context, sb *ffiwrapper.Sealer, sdr string, datadir string, par int, mid abi.ActorID, sectorSize abi.SectorSize) error {
	preCommit1Run := make(chan p1Run, par)
	running := make(chan struct{}, par)
	aprun := make(chan struct{}, 1)
	preCommit2Run := make(chan p2Run, 1024)
	apPiceRun := make(chan sectorTicket, par)
	stroageRun := make(chan storiface.SectorRef, 1024)
	go func() {
		for {
			//running <- struct{}{}
			aprun <- struct{}{}
			sql := `select sector_id,ticket from ` + db.TableTicket + ` where proving = 1 and running = 0 ORDER BY sector_id limit 1 `
			res, err := db.DBengine.QueryString(sql)
			if err != nil {
				//run = false
				log.Error("获取扇区数据失败：", err.Error())
				continue
			}
			if len(res) == 0 {
				//run = false
				log.Error("无可重做扇区")
				time.Sleep(time.Minute)
				continue
			}
			sql2 := `update ` + db.TableTicket + ` set running = 1 where sector_id=? and running<>1`

			res1, err1 := db.DBengine.Exec(sql2, res[0]["sector_id"])
			if err1 != nil {
				log.Error("修改状态错误:", err1.Error())
				continue
			}
			if row, _ := res1.RowsAffected(); row == 0 {
				log.Error("重复重做:", res[0]["sector_id"])
				continue
			}

			sn, err := strconv.ParseUint(res[0]["sector_id"], 10, 64)
			if err != nil {
				log.Error("扇区号有误：", err.Error())
				continue
			}
			if res[0]["ticket"] == "" {
				log.Error("ticket有误：", err.Error(), res[0]["sector_id"])
				continue
			}

			s := sectorTicket{
				storiface.SectorRef{
					ID: abi.SectorID{
						Miner:  mid,
						Number: abi.SectorNumber(sn),
					},
					ProofType: spt(sectorSize),
				},
				[]byte(res[0]["ticket"]),
			}
			apPiceRun <- s
		}
	}()
	//AP
	go func() {
		for {
			select {
			case sid := <-apPiceRun:
				log.Info("开始扇区：", sid.sector.ID, string(sid.ticket))
				go func(ticket sectorTicket) {

					p1info, err := addPiece(c, sb, ticket.sector, datadir)
					<-aprun
					if err != nil {
						log.Error("AP错误：", err.Error(), ",", ticket.sector.ID)
						return
					}
					p1 := p1Run{
						p1info,
						ticket,
					}
					preCommit1Run <- p1
				}(sid)
			}
		}
	}()
	//P1
	go func() {
		for {
			select {
			case p1 := <-preCommit1Run:
				running <- struct{}{}
				go func(run p1Run) {
					p1out, err := runP1(sb, run.st.sector, run.st.ticket, run.pinfo)
					<-running
					if err != nil {
						log.Error("P1错误：", err.Error(), ",", run.st.sector.ID)
						return
					}
					p2 := p2Run{
						p1out,
						p1.st.sector,
					}
					preCommit2Run <- p2
				}(p1)
			}
		}
	}()
	//P2
	go func() {
		for {
			select {
			case p2 := <-preCommit2Run:
				if err := runP2(sb, p2.sector, p2.pout); err != nil {
					log.Error("P2错误：", err.Error(), ",", p2.sector.ID)
					continue
				}
				stroageRun <- p2.sector
			}
		}
	}()
	//fin
	for {
		select {
		case si := <-stroageRun:
			go func() {
				moveStorage(sb, sdr, si, UnSealed|Cache|Sealed)
			}()
		}
	}
}

func addPiece(ctx context.Context, sb *ffiwrapper.Sealer, sp storiface.SectorRef, dir string) ([]abi.PieceInfo, error) {

	var offset abi.UnpaddedPieceSize
	var pieceSizes []abi.UnpaddedPieceSize
	var pieces []abi.PieceInfo

	//获取sb
	sql := `select * from ` + db.TablePiece + ` where sector_id = ? `
	res, err := db.DBengine.QueryString(sql, sp.ID.Number)
	if err != nil {
		return nil, err
	}

	for _, val := range res {
		size, _ := strconv.ParseUint(val["piece_size"], 10, 64)
		pcid, _ := cid.Decode(val["piece_cid"])
		piece := abi.PieceInfo{
			Size:     abi.PaddedPieceSize(size),
			PieceCID: pcid,
		}
		if val["deal_id"] == "0" {
			pieces = append(pieces, piece)
			continue
		}
		filepath := path.Join(dir, val["piece_cid"]+".car")
		st, err := os.Stat(filepath)
		if err != nil {
			return nil, err
		}

		v2r, err := carv2.OpenReader(filepath)
		if err != nil {
			return nil, err
		}
		defer func() {
			if err := v2r.Close(); err != nil {
				log.Warn("err:", err)
			}
		}()

		r, err := v2r.DataReader()
		if err != nil {
			return nil, fmt.Errorf("failed to get data reader over CAR file: %w", err)
		}
		paddedReader, err := padreader.NewInflator(r, uint64(st.Size()), piece.Size.Unpadded())
		if err != nil {
			return nil, fmt.Errorf("failed to create inflator: %w", err)
		}

		ppi, err := sb.AddPiece(ctx,
			sp,
			pieceSizes,
			piece.Size.Unpadded(),
			paddedReader)
		if err != nil {
			err = xerrors.Errorf("writing piece: %w", err)
			return nil, err
		}
		pieces = append(pieces, ppi)
		log.Infow("deal added to a sector", "deal", val["deal_id"], "sector", sp.ID, "piece", ppi.PieceCID)

		offset += ppi.Size.Unpadded()
		pieceSizes = append(pieceSizes, ppi.Size.Unpadded())
	}
	return pieces, nil

}
