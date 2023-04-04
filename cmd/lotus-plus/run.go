package main

import (
	"bytes"
	"context"
	"fmt"
	ffi "github.com/filecoin-project/filecoin-ffi"
	"github.com/filecoin-project/go-address"
	commpffi "github.com/filecoin-project/go-commp-utils/ffiwrapper"
	"github.com/filecoin-project/go-commp-utils/zerocomm"
	"github.com/filecoin-project/go-padreader"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/api"
	lcli "github.com/filecoin-project/lotus/cli"
	"github.com/filecoin-project/lotus/storage/pipeline/lib/nullreader"
	"github.com/filecoin-project/lotus/storage/sealer/ffiwrapper"
	"github.com/filecoin-project/lotus/storage/sealer/ffiwrapper/basicfs"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
	"github.com/ipfs/go-cid"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/urfave/cli/v2"
	"io"

	"github.com/mitchellh/go-homedir"
	"golang.org/x/xerrors"
	"os"
)

var sealRecoverCmd = &cli.Command{
	Name:  "run",
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
			Usage: "原值扇区文件路径",
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
		&cli.Uint64Flag{
			Name:  "sids",
			Usage: "扇区ID列表：0,1,2,3,4",
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
		nodeAPI, closer, err := lcli.GetStorageMinerAPI(c)
		if err != nil {
			return err
		}
		defer closer()
		sectorNumer := abi.SectorNumber(c.Uint64("sids"))
		sectorInfo, err := nodeAPI.SectorsStatus(c.Context, sectorNumer, false)
		if err != nil {
			return err
		}

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
		//sectorSizeInt, err := units.RAMInBytes(c.String("sector-size"))
		//if err != nil {
		//	return err
		//}
		//sectorSize := abi.SectorSize(sectorSizeInt)

		sbfs := &basicfs.Provider{
			Root: sdir,
		}

		sb, err := ffiwrapper.New(sbfs)
		if err != nil {
			return err
		}

		//err = runSeals(sb,sdir, c.Int("parallel"), mid, sectorSize)
		//if err != nil {
		//	return xerrors.Errorf("failed to run seals: %w", err)
		//}
		maddr, err := address.NewFromString(c.String("miner-addr"))
		if err != nil {
			return err
		}
		amid, err := address.IDFromAddress(maddr)
		if err != nil {
			return err
		}
		aid := abi.SectorID{
			abi.ActorID(amid),
			sectorNumer,
		}
		sector := storiface.SectorRef{
			ID:        aid,
			ProofType: sectorInfo.SealProof,
		}

		InboundFilePath := c.String("datapath")
		v2r, err := carv2.OpenReader(InboundFilePath)
		if err != nil {
			return err
		}
		defer func() {
			if err := v2r.Close(); err != nil {
				log.Warn("err:", err)
			}
		}()

		var size uint64
		switch v2r.Version {
		case 1:
			st, err := os.Stat(InboundFilePath)
			if err != nil {
				return err
			}
			size = uint64(st.Size())
			log.Info(v2r.Version, "size:", size)
		case 2:
			size = v2r.Header.DataSize
			log.Info(v2r.Version, "size:", size)
		}

		r, err := v2r.DataReader()
		if err != nil {
			return fmt.Errorf("failed to get data reader over CAR file: %w", err)
		}
		paddedReader, err := padreader.NewInflator(r, size, sectorInfo.Pieces[0].Piece.Size.Unpadded())
		if err != nil {
			return fmt.Errorf("failed to create inflator: %w", err)
		}

		//paddedReader, err := Read(c.String("datapath"), sectorInfo.Pieces[0].Piece.Size)
		//if err != nil {
		//	return err
		//}
		//sb.AddPiece(c.Context,sector,)
		pis, err := handleAddPiece(c.Context, sb, sectorInfo, sector, paddedReader)
		if err != nil {
			return err
		}
		p1, err := sb.SealPreCommit1(c.Context, sector, sectorInfo.Ticket.Value, pis)
		if err != nil {
			return err
		}
		_, err = sb.SealPreCommit2(c.Context, sector, p1)
		if err != nil {
			return err
		}
		sb.FinalizeSector(c.Context, sector, nil)
		return err
	},
}

func Read(InboundFilePath string, piece abi.PaddedPieceSize) (io.Reader, error) {
	v2r, err := carv2.OpenReader(InboundFilePath)
	if err != nil {
		return nil, err
	}
	//defer func() {
	//	if err := v2r.Close(); err != nil {
	//		log.Warn("err:", err)
	//	}
	//}()

	var size uint64
	switch v2r.Version {
	case 1:
		st, err := os.Stat(InboundFilePath)
		if err != nil {
			return nil, err
		}
		size = uint64(st.Size())
		log.Info(v2r.Version, "size:", size)
	case 2:
		size = v2r.Header.DataSize
		log.Info(v2r.Version, "size:", size)
	}

	r, err := v2r.DataReader()
	if err != nil {
		return nil, fmt.Errorf("failed to get data reader over CAR file: %w", err)
	}
	paddedReader, err := padreader.NewInflator(r, size, piece.Unpadded())
	if err != nil {
		return nil, fmt.Errorf("failed to create inflator: %w", err)
	}
	return paddedReader, nil
}

func handleAddPiece(ctx context.Context, sb *ffiwrapper.Sealer, sector api.SectorInfo, sp storiface.SectorRef, pieceData io.Reader) ([]abi.PieceInfo, error) {

	var offset abi.UnpaddedPieceSize
	var pieceSizes []abi.UnpaddedPieceSize
	var pieces []abi.PieceInfo
	for k, piece := range sector.Pieces {

		pads, padLength := ffiwrapper.GetRequiredPadding(offset.Padded(), piece.Piece.Size)
		log.Info(k, ",padLength:", padLength)
		offset += padLength.Unpadded()
		log.Info(k, "offset:", offset)
		for j, p := range pads {
			expectCid := zerocomm.ZeroPieceCommitment(p.Unpadded())
			log.Info(k, ",", j, "pads:", pads, ",expectCid:", expectCid)
			ppi, err := sb.AddPiece(ctx,
				sp,
				pieceSizes,
				p.Unpadded(),
				nullreader.NewNullReader(p.Unpadded()))
			if err != nil {
				err = xerrors.Errorf("writing padding piece: %w", err)
				return nil, err
			}

			if !ppi.PieceCID.Equals(expectCid) {
				err = xerrors.Errorf("got unexpected padding piece CID: expected:%s, got:%s", expectCid, ppi.PieceCID)
				return nil, err
			}
			pieces = append(pieces, ppi)
			pieceSizes = append(pieceSizes, p.Unpadded())
			log.Info(j, "expectCid:", expectCid, ",", ppi.PieceCID, ",", pieceSizes, ",", p.Unpadded())
			offset += ppi.Size.Unpadded()
			pieceSizes = append(pieceSizes, ppi.Size.Unpadded())
			continue
		}
		if sector.Deals[k] != 0 {
			ppi, err := sb.AddPiece(ctx,
				sp,
				pieceSizes,
				piece.Piece.Size.Unpadded(),
				pieceData)
			if err != nil {
				err = xerrors.Errorf("writing piece: %w", err)
				return nil, err
			}
			pieces = append(pieces, ppi)
			log.Infow("deal added to a sector", "deal", piece.DealInfo.DealID, "sector", sector.SectorID, "piece", ppi.PieceCID, ":ppi.size:", ppi.Size.Unpadded())

			offset += ppi.Size.Unpadded()
			pieceSizes = append(pieceSizes, ppi.Size.Unpadded())
			continue
		}
		pieces = append(pieces, piece.Piece)
		offset += piece.Piece.Size.Unpadded()
		pieceSizes = append(pieceSizes, piece.Piece.Size.Unpadded())
	}
	return pieces, nil

}

//func AddPiece(ctx context.Context, spt abi.RegisteredSealProof, existingPieceSizes []abi.UnpaddedPieceSize, pieceSize abi.UnpaddedPieceSize, file storage.Data,sdir string) (abi.PieceInfo, error) {
//	// TODO: allow tuning those:
//	chunk := abi.PaddedPieceSize(4 << 20)
//	parallel := runtime.NumCPU()
//
//	var offset abi.UnpaddedPieceSize
//	for _, size := range existingPieceSizes {
//		offset += size
//	}
//
//	ssize, err := spt.SectorSize()
//	if err != nil {
//		return abi.PieceInfo{}, err
//	}
//
//	maxPieceSize := abi.PaddedPieceSize(ssize)
//
//	if offset.Padded()+pieceSize.Padded() > maxPieceSize {
//		return abi.PieceInfo{}, xerrors.Errorf("can't add %d byte piece to sector %v with %d bytes of existing pieces", pieceSize, offset)
//	}
//
//	var done func()
//	var stagedFile *partialfile.PartialFile
//
//	defer func() {
//		if done != nil {
//			done()
//		}
//		if stagedFile != nil {
//			if err := stagedFile.Close(); err != nil {
//				log.Errorf("closing staged file: %+v", err)
//			}
//		}
//	}()
//
//	//var stagedPath storiface.SectorPaths
//	Unsealed := sdir
//	if len(existingPieceSizes) == 0 {
//
//		stagedFile, err = partialfile.CreatePartialFile(maxPieceSize, stagedPath.Unsealed)
//		if err != nil {
//			return abi.PieceInfo{}, xerrors.Errorf("creating unsealed sector file: %w", err)
//		}
//	} else {
//		stagedFile, err = partialfile.OpenPartialFile(maxPieceSize, stagedPath.Unsealed)
//		if err != nil {
//			return abi.PieceInfo{}, xerrors.Errorf("opening unsealed sector file: %w", err)
//		}
//	}
//
//
//	w, err := stagedFile.Writer(storiface.UnpaddedByteIndex(offset).Padded(), pieceSize.Padded())
//	if err != nil {
//		return abi.PieceInfo{}, xerrors.Errorf("getting partial file writer: %w", err)
//	}
//
//	pw := fr32.NewPadWriter(w)
//
//	pr := io.TeeReader(io.LimitReader(file, int64(pieceSize)), pw)
//
//	throttle := make(chan []byte, parallel)
//	piecePromises := make([]func() (abi.PieceInfo, error), 0)
//
//	buf := make([]byte, chunk.Unpadded())
//	for i := 0; i < parallel; i++ {
//		if abi.UnpaddedPieceSize(i)*chunk.Unpadded() >= pieceSize {
//			break // won't use this many buffers
//		}
//		throttle <- make([]byte, chunk.Unpadded())
//	}
//
//	for {
//		var read int
//		for rbuf := buf; len(rbuf) > 0; {
//			n, err := pr.Read(rbuf)
//			if err != nil && err != io.EOF {
//				return abi.PieceInfo{}, xerrors.Errorf("pr read error: %w", err)
//			}
//
//			rbuf = rbuf[n:]
//			read += n
//
//			if err == io.EOF {
//				break
//			}
//		}
//		if read == 0 {
//			break
//		}
//
//		done := make(chan struct {
//			cid.Cid
//			error
//		}, 1)
//		pbuf := <-throttle
//		copy(pbuf, buf[:read])
//
//		go func(read int) {
//			defer func() {
//				throttle <- pbuf
//			}()
//
//			c, err := pieceCid(spt, pbuf[:read])
//			done <- struct {
//				cid.Cid
//				error
//			}{c, err}
//		}(read)
//
//		piecePromises = append(piecePromises, func() (abi.PieceInfo, error) {
//			select {
//			case e := <-done:
//				if e.error != nil {
//					return abi.PieceInfo{}, e.error
//				}
//
//				return abi.PieceInfo{
//					Size:     abi.UnpaddedPieceSize(len(buf[:read])).Padded(),
//					PieceCID: e.Cid,
//				}, nil
//			case <-ctx.Done():
//				return abi.PieceInfo{}, ctx.Err()
//			}
//		})
//	}
//
//	if err := pw.Close(); err != nil {
//		return abi.PieceInfo{}, xerrors.Errorf("closing padded writer: %w", err)
//	}
//
//	if err := stagedFile.MarkAllocated(storiface.UnpaddedByteIndex(offset).Padded(), pieceSize.Padded()); err != nil {
//		return abi.PieceInfo{}, xerrors.Errorf("marking data range as allocated: %w", err)
//	}
//
//	if err := stagedFile.Close(); err != nil {
//		return abi.PieceInfo{}, err
//	}
//	stagedFile = nil
//
//	if len(piecePromises) == 1 {
//		return piecePromises[0]()
//	}
//
//	var payloadRoundedBytes abi.PaddedPieceSize
//	pieceCids := make([]abi.PieceInfo, len(piecePromises))
//	for i, promise := range piecePromises {
//		pinfo, err := promise()
//		if err != nil {
//			return abi.PieceInfo{}, err
//		}
//
//		pieceCids[i] = pinfo
//		payloadRoundedBytes += pinfo.Size
//	}
//
//	pieceCID, err := ffi.GenerateUnsealedCID(spt, pieceCids)
//	if err != nil {
//		return abi.PieceInfo{}, xerrors.Errorf("generate unsealed CID: %w", err)
//	}
//
//	// validate that the pieceCID was properly formed
//	if _, err := commcid.CIDToPieceCommitmentV1(pieceCID); err != nil {
//		return abi.PieceInfo{}, err
//	}
//
//	if payloadRoundedBytes < pieceSize.Padded() {
//		paddedCid, err := commpffi.ZeroPadPieceCommitment(pieceCID, payloadRoundedBytes.Unpadded(), pieceSize)
//		if err != nil {
//			return abi.PieceInfo{}, xerrors.Errorf("failed to pad data: %w", err)
//		}
//
//		pieceCID = paddedCid
//	}
//
//	return abi.PieceInfo{
//		Size:     pieceSize.Padded(),
//		PieceCID: pieceCID,
//	}, nil
//}

func pieceCid(spt abi.RegisteredSealProof, in []byte) (cid.Cid, error) {
	prf, werr, err := commpffi.ToReadableFile(bytes.NewReader(in), int64(len(in)))
	if err != nil {
		return cid.Undef, xerrors.Errorf("getting tee reader pipe: %w", err)
	}

	pieceCID, err := ffi.GeneratePieceCIDFromFile(spt, prf, abi.UnpaddedPieceSize(len(in)))
	if err != nil {
		return cid.Undef, xerrors.Errorf("generating piece commitment: %w", err)
	}

	_ = prf.Close()

	return pieceCID, werr()
}
