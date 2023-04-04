package sealing

import (
	"context"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/lotus/storage/sealer/storiface"
)

//yungojs
func (m *Sealing) PledgeSector(ctx context.Context,cur uint64) (storiface.SectorRef, error) {
	m.startupWait.Wait()

	m.inputLk.Lock()
	defer m.inputLk.Unlock()

	cfg, err := m.getConfig()
	if err != nil {
		return storiface.SectorRef{}, xerrors.Errorf("getting config: %w", err)
	}

	if cfg.MaxSealingSectors > 0 {
		if m.stats.curSealing() >= cfg.MaxSealingSectors {
			return storiface.SectorRef{}, xerrors.Errorf("too many sectors sealing (curSealing: %d, max: %d)", m.stats.curSealing(), cfg.MaxSealingSectors)
		}
	}

	spt, err := m.currentSealProof(ctx)
	if err != nil {
		return storiface.SectorRef{}, xerrors.Errorf("getting seal proof type: %w", err)
	}

	//yungojs
	sid, err := m.createSector(ctx, cfg, spt,cur)
	if err != nil {
		return storiface.SectorRef{}, err
	}

	log.Infof("Creating CC sector %d", sid)
	return m.minerSector(spt, sid), m.sectors.Send(uint64(sid), SectorStartCC{
		ID:         sid,
		SectorType: spt,
	})
}
