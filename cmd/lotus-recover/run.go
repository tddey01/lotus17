package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/actors/builtin/miner"
	server_c2 "github.com/filecoin-project/lotus/extern/server-c2"
	"github.com/filecoin-project/lotus/storage/db"
	"github.com/filecoin-project/lotus/storage/pipeline/lib/nullreader"
	"github.com/filecoin-project/lotus/storage/sealer/ffiwrapper"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var sealRecoverCmd = &cli.Command{
	Name:  "run",
	Usage: "Benchmark seal and winning post and window post",
	Subcommands: []*cli.Command{
		sealCcCmd,
		sealDcCmd,
	},
}

const (
	ss512MiB = 512 << 20
	ss32GiB  = 32 << 30
	ss64GiB  = 64 << 30
)
const (
	UnSealed = 1 << iota
	Cache
	Sealed
)

var fileTypes = []int{UnSealed, Cache, Sealed}

type p1Run struct {
	pinfo []abi.PieceInfo
	st    sectorTicket
}
type p2Run struct {
	pout   storiface.PreCommit1Out
	sector storiface.SectorRef
}
type sectorTicket struct {
	sector storiface.SectorRef
	ticket []byte
}

func runSeals(sb *ffiwrapper.Sealer, sdr string, par int, mid abi.ActorID, sectorSize abi.SectorSize) error {
	preCommit1Run := make(chan p1Run, par)
	preCommit1Finish := make(chan struct{}, 1)
	preCommit2Run := make(chan p2Run, 1024)
	apPiceRun := make(chan sectorTicket, par)
	stroageRun := make(chan storiface.SectorRef, 1024)
	go func() {
		var i = 0
		var run = true
		for {
			if i >= par && run {
				<-preCommit1Finish
			}
			sql := `select sector_id,ticket from ` + db.TableTicket + ` where proving = 1 and running = 0 ORDER BY sector_id limit 1 `
			res, err := db.DBengine.QueryString(sql)
			if err != nil {
				run = false
				log.Error("获取扇区数据失败：", err.Error())
				continue
			}
			if len(res) == 0 {
				run = false
				log.Error("无可重做扇区")
				time.Sleep(time.Minute)
				continue
			}
			sql2 := `update ` + db.TableTicket + ` set running = 1 where sector_id=? and running<>1`

			res1, err1 := db.DBengine.Exec(sql2, res[0]["sector_id"])
			if err1 != nil {
				run = false
				log.Error("修改状态错误:", err1.Error())
				continue
			}
			if row, _ := res1.RowsAffected(); row == 0 {
				run = false
				log.Error("重复重做:", res[0]["sector_id"])
				continue
			}
			sn, err := strconv.ParseUint(res[0]["sector_id"], 10, 64)
			if err != nil {
				run = false
				log.Error("扇区号有误：", err.Error())
				continue
			}
			if res[0]["ticket"] == "" {
				run = false
				log.Error("ticket有误：", err.Error(), res[0]["sector_id"])
				continue
			}
			i++
			run = true
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
					p1info, err := runAp(sb, ticket.sector, sectorSize)
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
				go func(run p1Run) {
					p1out, err := runP1(sb, run.st.sector, run.st.ticket, run.pinfo)
					defer func() { preCommit1Finish <- struct{}{} }()
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
				moveStorage(sb, sdr, si, Cache|Sealed)
			}()
		}
	}
}

func runAp(sb *ffiwrapper.Sealer, sid storiface.SectorRef, sectorSize abi.SectorSize) ([]abi.PieceInfo, error) {
	log.Infof("[%d] Writing piece into sector...", sid.ID)

	r := nullreader.NewNullReader(abi.UnpaddedPieceSize(sectorSize))
	linkpath := ""
	var pi abi.PieceInfo
	var err error
	switch sectorSize {
	case ss512MiB:
		linkpath = os.Getenv("MINER512_S512M_PATH")
		if linkpath != "" {
			pi, err = sb.AddPiece2(context.TODO(), sid, nil, abi.PaddedPieceSize(sectorSize).Unpadded(), linkpath)
			if err != nil {
				return nil, err
			}
		}
	case ss32GiB:
		linkpath = os.Getenv("MINER32_S32G_PATH")
		if linkpath != "" {
			pi, err = sb.AddPiece2(context.TODO(), sid, nil, abi.PaddedPieceSize(sectorSize).Unpadded(), linkpath)
			if err != nil {
				return nil, err
			}
		}
	case ss64GiB:
		linkpath = os.Getenv("MINER64_S64G_PATH")
		if linkpath != "" {
			pi, err = sb.AddPiece2(context.TODO(), sid, nil, abi.PaddedPieceSize(sectorSize).Unpadded(), linkpath)
			if err != nil {
				return nil, err
			}
		}
	}
	if linkpath == "" {
		pi, err = sb.AddPiece(context.TODO(), sid, nil, abi.PaddedPieceSize(sectorSize).Unpadded(), r)
		if err != nil {
			return nil, err
		}
	}
	return []abi.PieceInfo{pi}, nil
}

func runP1(sb *ffiwrapper.Sealer, sid storiface.SectorRef, ticketPreimage []byte, piece []abi.PieceInfo) (storiface.PreCommit1Out, error) {

	//trand := blake2b.Sum256(ticketPreimage)
	//ticket := abi.SealRandomness(trand[:])
	ticket, _ := hex.DecodeString(string(ticketPreimage))

	log.Infof("[%d] Running replication(1)...", sid.ID.Number)

	return sb.SealPreCommit1(context.TODO(), sid, ticket, piece)
}

func runP2(sb *ffiwrapper.Sealer, sid storiface.SectorRef, pc1o storiface.PreCommit1Out) error {
	log.Infof("[%d] Running replication(2)...", sid.ID.Number)
	_, err := sb.SealPreCommit2(context.TODO(), sid, pc1o)
	if err != nil {
		return xerrors.Errorf("precommit2: %w", err)
	}
	sql2 := `update ` + db.TableTicket + ` set running = 2 where sector_id=?`

	if _, err = db.DBengine.Exec(sql2, sid.ID.Number.String()); err != nil {
		log.Error("修改扇区状态错误:", err.Error())
	}
	return nil
}

func moveStorage(sb *ffiwrapper.Sealer, sdr string, si storiface.SectorRef, flag int) {
	log.Info("待转移扇区：", si.ID)
	if err := sb.FinalizeSector(context.Background(), si, nil); err != nil {
		log.Error("fin错误：", err.Error())
		return
	}
	path := ""
	id := ""
	for {
		sql := `select path,id from ` + db.TableStorage + ` where running = 0 limit 1  `
		res, err := db.DBengine.QueryString(sql)
		if err != nil {
			log.Error("获取存储错误：", err.Error())
			time.Sleep(time.Second)
			continue
		}
		if len(res) == 0 {
			log.Error("未获取到存储")
			time.Sleep(time.Second * 10)
			continue
		}
		path = res[0]["path"]
		if path == "" {
			time.Sleep(time.Second)
			continue
		}
		var stat syscall.Statfs_t
		if err := syscall.Statfs(path, &stat); err != nil {
			fmt.Println(err)
		}
		size, _ := si.ProofType.SectorSize()
		bavail := 1
		if uint64(stat.Bavail)*uint64(stat.Bsize) < uint64(size)*2 {
			bavail = 2
		}

		id = res[0]["id"]
		sql1 := `update ` + db.TableStorage + ` set running = ? where id = ? `
		if _, err := db.DBengine.Exec(sql1, bavail, res[0]["id"]); err != nil {
			log.Error("修改存储失败！", err.Error())
			time.Sleep(time.Second)
			continue
		}

		if bavail == 2 {
			continue
		}
		break
	}
	defer func() {
		sql1 := `update ` + db.TableStorage + ` set running = 0 where id = ? `
		if _, err := db.DBengine.Exec(sql1, id); err != nil {
			log.Error("恢复存储状态失败！", err.Error())
		}
	}()

	for _, v := range fileTypes {
		switch flag & v {
		case UnSealed:
			unsealed := filepath.Join(sdr, "/unsealed/", server_c2.SectorNumString(si.ID))
			unsealedStorage := filepath.Join(path, "/lotusminer/unsealed/")
			log.Infof("开始转移：unsealed: %s ， to：%s", unsealed, unsealedStorage)
			if err := move(unsealed, unsealedStorage); err != nil {
				log.Error("转移失败！:", unsealed, ",", path, ",", err.Error())
				return
			}
		case Cache:
			cache := filepath.Join(sdr, "/cache/", server_c2.SectorNumString(si.ID))
			cacheStorage := filepath.Join(path, "/lotusminer/cache/")
			log.Infof("开始转移：cache: %s ， to：%s", cache, cacheStorage)
			if err := move(cache, cacheStorage); err != nil {
				log.Error("转移失败！:", cache, ",", cacheStorage, ",", err.Error())
				return
			}
		case Sealed:
			sealed := filepath.Join(sdr, "/sealed/", server_c2.SectorNumString(si.ID))
			sealedStorage := filepath.Join(path, "/lotusminer/sealed/")
			log.Infof("开始转移：sealed: %s ， to：%s", sealed, sealedStorage)
			if err := move(sealed, sealedStorage); err != nil {
				log.Error("转移失败！:", sealed, ",", path, ",", err.Error())
				return
			}
		}
	}

	log.Info("已完成扇区:", si.ID)
}
func spt(ssize abi.SectorSize) abi.RegisteredSealProof {
	spt, err := miner.SealProofTypeFromSectorSize(ssize, build.NewestNetworkVersion)
	if err != nil {
		panic(err)
	}

	return spt
}
func move(from, to string) error {

	log.Info("move sector data ", "from: ", from, "to: ", to)
	// `mv` has decades of experience in moving files quickly; don't pretend we
	//  can do better

	var errOut bytes.Buffer

	var cmd *exec.Cmd
	cmd = exec.Command("/usr/bin/env", "mv", "-t", to, from)

	cmd.Stderr = &errOut
	if err := cmd.Run(); err != nil {
		return xerrors.Errorf("exec mv (stderr: %s): %w", strings.TrimSpace(errOut.String()), err)
	}

	return nil
}
