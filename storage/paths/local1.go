package paths

import (
	"context"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
	"golang.org/x/xerrors"
)

//yungojs
func (st *Local) AcquireSector1(ctx context.Context, sid storiface.SectorRef, existing storiface.SectorFileType, allocate storiface.SectorFileType, pathType storiface.PathType, op storiface.AcquireMode) (storiface.SectorPaths, storiface.SectorPaths, error) {
	if existing|allocate != existing^allocate {
		return storiface.SectorPaths{}, storiface.SectorPaths{}, xerrors.New("can't both find and allocate a sector")
	}

	ssize, err := sid.ProofType.SectorSize()
	if err != nil {
		return storiface.SectorPaths{}, storiface.SectorPaths{}, err
	}

	st.localLk.RLock()
	defer st.localLk.RUnlock()

	var out storiface.SectorPaths
	var storageIDs storiface.SectorPaths
	//第二遍 existing != 0 allocate == 0
	for _, fileType := range storiface.PathTypes {
		if fileType&existing == 0 {
			continue
		}
		//获取扇区存储路径
		si, err := st.index.StorageFindSector(ctx, sid.ID, fileType, ssize, false)
		if err != nil {
			log.Warnf("finding existing sector %d(t:%d) failed: %+v", sid, fileType, err)
			continue
		}
		for i, v := range si {
			p, ok := st.paths[v.ID]
			if !ok {
				log.Info(i, "找到文件路径：", sid.ID, ",", fileType, ",find-id:", v.ID, ",不存在本地")
				continue
			}
			log.Info(i, "找到文件路径：", sid.ID, ",", fileType, ",find-id:", v.ID, ",", p.local)
		}
		for _, info := range si {
			p, ok := st.paths[info.ID]
			if !ok {
				continue
			}

			if p.local == "" { // TODO: can that even be the case?
				continue
			}

			spath := p.sectorPath(sid.ID, fileType)
			storiface.SetPathByType(&out, fileType, spath)
			storiface.SetPathByType(&storageIDs, fileType, string(info.ID))

			existing ^= fileType
			break
		}
	}
	//第一遍 existing == 0 allocate != 0
	for _, fileType := range storiface.PathTypes {
		if fileType&allocate == 0 {
			continue
		}
		//对存储进行排序 选择存储
		sis, err := st.index.StorageBestAlloc(ctx, fileType, ssize, pathType)
		if err != nil {
			return storiface.SectorPaths{}, storiface.SectorPaths{}, xerrors.Errorf("finding best storage for allocating : %w", err)
		}

		var best string
		var bestID storiface.ID
		st.sectorsRL.Lock()
		for i, si := range sis {

			if si.LocalPath == "" { // TODO: can that even be the case?
				continue
			}
			if !si.PathExists() {
				log.Info("中创存储6：", si.LocalPath, "不存在")
				continue
			}

			if (pathType == storiface.PathSealing) && !si.CanSeal {
				continue
			}

			if (pathType == storiface.PathStorage) && !si.CanStore {
				continue
			}
			//log.Info("存储扇区：", sid.ID, "，类型：", fileType, "，", st.sectors[sid.ID], ",", si.ID)
			if i < len(sis)-1 {
				if _, ok := st.sectors[sid.ID]; ok && st.sectors[sid.ID] != si.ID {
					continue
				}
			}

			// TODO: Check free space
			best = si.SectorPath(sid.ID, fileType)
			bestID = si.ID
			//log.Info(i, "保存扇区：", sid.ID, "，类型：", fileType, "，", st.sectors[sid.ID], ",", si.ID)
			st.sectors[sid.ID] = si.ID

			break
		}
		st.sectorsRL.Unlock()

		if best == "" {
			return storiface.SectorPaths{}, storiface.SectorPaths{}, xerrors.Errorf("couldn't find a suitable path for a sector1 %d", len(sis))
		}

		storiface.SetPathByType(&out, fileType, best)
		storiface.SetPathByType(&storageIDs, fileType, string(bestID))
		allocate ^= fileType

		//yungojs
		if err := st.index.StorageSetWeight(ctx, storiface.ID(storiface.PathByType(storageIDs, fileType))); err != nil {
			log.Warnf("修改权重失败: %w", err)
		}
	}

	return out, storageIDs, nil
}
