package kit

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	libp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/require"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/chain/wallet/key"
	"github.com/filecoin-project/lotus/miner"
	"github.com/filecoin-project/lotus/storage/paths"
	sealing "github.com/filecoin-project/lotus/storage/pipeline"
	"github.com/filecoin-project/lotus/storage/sealer/storiface"
)

type MinerSubsystem int

const (
	SMarkets MinerSubsystem = 1 << iota
	SMining
	SSealing
	SSectorStorage

	MinerSubsystems = iota
)

func (ms MinerSubsystem) Add(single MinerSubsystem) MinerSubsystem {
	return ms | single
}

func (ms MinerSubsystem) Has(single MinerSubsystem) bool {
	return ms&single == single
}

func (ms MinerSubsystem) All() [MinerSubsystems]bool {
	var out [MinerSubsystems]bool

	for i := range out {
		out[i] = ms&(1<<i) > 0
	}

	return out
}

// TestMiner represents a miner enrolled in an Ensemble.
type TestMiner struct {
	api.StorageMiner

	BaseAPI api.StorageMiner

	t *testing.T

	// ListenAddr is the address on which an API server is listening, if an
	// API server is created for this Node
	ListenAddr multiaddr.Multiaddr

	ActorAddr address.Address
	OwnerKey  *key.Key
	MineOne   func(context.Context, miner.MineReq) error
	Stop      func(context.Context) error

	FullNode       *TestFullNode
	PresealDir     string
	PresealSectors int

	Libp2p struct {
		PeerID  peer.ID
		PrivKey libp2pcrypto.PrivKey
	}

	RemoteListener net.Listener

	options nodeOpts
}

func (tm *TestMiner) PledgeSectors(ctx context.Context, n, existing int, blockNotif <-chan struct{}) {
	toCheck := tm.StartPledge(ctx, n, existing, blockNotif)
	tm.WaitSectorsProving(ctx, toCheck)
}

func (tm *TestMiner) WaitSectorsProving(ctx context.Context, toCheck map[abi.SectorNumber]struct{}) {
	tm.WaitSectorsProvingAllowFails(ctx, toCheck, map[api.SectorState]struct{}{})
}

func (tm *TestMiner) WaitSectorsProvingAllowFails(ctx context.Context, toCheck map[abi.SectorNumber]struct{}, okFails map[api.SectorState]struct{}) {
	for len(toCheck) > 0 {
		tm.FlushSealingBatches(ctx)

		states := map[api.SectorState]int{}
		for n := range toCheck {
			st, err := tm.StorageMiner.SectorsStatus(ctx, n, false)
			require.NoError(tm.t, err)
			states[st.State]++
			if st.State == api.SectorState(sealing.Proving) || st.State == api.SectorState(sealing.Available) || st.State == api.SectorState(sealing.Removed) {
				delete(toCheck, n)
			}
			if strings.Contains(string(st.State), "Fail") {
				if _, ok := okFails[st.State]; !ok {
					tm.t.Fatal("sector in a failed state", st.State)
				}
			}
		}

		build.Clock.Sleep(100 * time.Millisecond)
		fmt.Printf("WaitSectorsProving: %d %+v\n", len(toCheck), states)
	}
}

func (tm *TestMiner) StartPledge(ctx context.Context, n, existing int, blockNotif <-chan struct{}) map[abi.SectorNumber]struct{} {
	for i := 0; i < n; i++ {
		if i%3 == 0 && blockNotif != nil {
			<-blockNotif
			tm.t.Log("WAIT")
		}
		tm.t.Logf("PLEDGING %d", i)
		//yungojs
		_, err := tm.StorageMiner.PledgeSector(ctx, 0)
		require.NoError(tm.t, err)
	}

	for {
		s, err := tm.SectorsListNonGenesis(ctx)
		require.NoError(tm.t, err)
		fmt.Printf("Sectors: %d (n %d, ex %d)\n", len(s), n, existing)
		if len(s) >= n+existing {
			break
		}

		build.Clock.Sleep(100 * time.Millisecond)
	}

	fmt.Printf("All sectors is fsm\n")

	s, err := tm.SectorsListNonGenesis(ctx)
	require.NoError(tm.t, err)

	toCheck := map[abi.SectorNumber]struct{}{}
	for _, number := range s {
		toCheck[number] = struct{}{}
	}

	return toCheck
}

func (tm *TestMiner) FlushSealingBatches(ctx context.Context) {
	pcb, err := tm.StorageMiner.SectorPreCommitFlush(ctx)
	require.NoError(tm.t, err)
	if pcb != nil {
		fmt.Printf("PRECOMMIT BATCH: %+v\n", pcb)
	}

	cb, err := tm.StorageMiner.SectorCommitFlush(ctx)
	require.NoError(tm.t, err)
	if cb != nil {
		fmt.Printf("COMMIT BATCH: %+v\n", cb)
	}
}

const metaFile = "sectorstore.json"

func (tm *TestMiner) AddStorage(ctx context.Context, t *testing.T, conf func(*paths.LocalStorageMeta)) storiface.ID {
	p := t.TempDir()

	if err := os.MkdirAll(p, 0755); err != nil {
		if !os.IsExist(err) {
			require.NoError(t, err)
		}
	}

	_, err := os.Stat(filepath.Join(p, metaFile))
	if !os.IsNotExist(err) {
		require.NoError(t, err)
	}

	cfg := &paths.LocalStorageMeta{
		ID:       storiface.ID(uuid.New().String()),
		Weight:   10,
		CanSeal:  false,
		CanStore: false,
	}

	conf(cfg)

	if !(cfg.CanStore || cfg.CanSeal) {
		t.Fatal("must specify at least one of CanStore or cfg.CanSeal")
	}

	b, err := json.MarshalIndent(cfg, "", "  ")
	require.NoError(t, err)

	err = ioutil.WriteFile(filepath.Join(p, metaFile), b, 0644)
	require.NoError(t, err)

	err = tm.StorageAddLocal(ctx, p)
	require.NoError(t, err)

	return cfg.ID
}
func (tm *TestMiner) SectorsListNonGenesis(ctx context.Context) ([]abi.SectorNumber, error) {
	l, err := tm.SectorsList(ctx)
	if err != nil {
		return nil, err
	}
	// sort just in case
	sort.Slice(l, func(i, j int) bool {
		return l[i] < l[j]
	})

	return l[tm.PresealSectors:], nil
}
