package ffiwrapper

import (
	"context"
	"errors"
	"github.com/filecoin-project/go-commp-utils/zerocomm"
	"github.com/filecoin-project/go-state-types/abi"
	"golang.org/x/xerrors"
	"os"

	"github.com/filecoin-project/lotus/storage/sealer/storiface"
)

const (
	ss2KiB   = 2 << 10
	ss8MiB   = 8 << 20
	ss512MiB = 512 << 20
	ss32GiB  = 32 << 30
	ss64GiB  = 64 << 30
)

func (sb *Sealer) AddPiece2(ctx context.Context, sector storiface.SectorRef, existingPieceSizes []abi.UnpaddedPieceSize, pieceSize abi.UnpaddedPieceSize, linkpath string) (abi.PieceInfo, error) {
	// TODO: allow tuning those:
	log.Info("生成硬链接文件AddPiece1:", len(existingPieceSizes), ",pieceSize:", pieceSize)
	var offset abi.UnpaddedPieceSize
	for _, size := range existingPieceSizes {
		offset += size
	}

	ssize, err := sector.ProofType.SectorSize()
	if err != nil {
		return abi.PieceInfo{}, err
	}

	maxPieceSize := abi.PaddedPieceSize(ssize)

	if offset.Padded()+pieceSize.Padded() > maxPieceSize {
		return abi.PieceInfo{}, xerrors.Errorf("can't add %d byte piece to sector %v with %d bytes of existing pieces", pieceSize, sector, offset)
	}

	var done func()

	defer func() {
		if done != nil {
			done()
		}
	}()

	var stagedPath storiface.SectorPaths
	stagedPath, done, err = sb.sectors.AcquireSector(ctx, sector, 0, storiface.FTUnsealed, storiface.PathSealing)
	if err != nil {
		return abi.PieceInfo{}, xerrors.Errorf("acquire unsealed sector: %w", err)
	}

	err = createFile(linkpath, stagedPath.Unsealed)
	if err != nil {
		return abi.PieceInfo{}, xerrors.Errorf("创建硬链接失败: %w", err)
	}
	log.Info("创建硬链接完成!", sector.ID.Number)
	return abi.PieceInfo{
		Size:     pieceSize.Padded(),
		PieceCID: zerocomm.ZeroPieceCommitment(pieceSize),
	}, nil
}
func createFile(linkpath, path string) error {
	if path == "" {
		return errors.New("路径为空!")
	}
	if b, _ := PathExists(path); b {
		return nil
	}

	return os.Link(linkpath, path)
}
func PathExists(path string) (bool, error) {

	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
