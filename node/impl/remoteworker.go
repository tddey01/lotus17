package impl

import (
	"context"
	"github.com/filecoin-project/lotus/storage/sealer/sealtasks"
	"net/http"
	"strings"

	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/go-jsonrpc/auth"
	"github.com/filecoin-project/go-state-types/abi"

	"github.com/filecoin-project/lotus/api"
	"github.com/filecoin-project/lotus/api/client"
	"github.com/filecoin-project/lotus/storage/sealer"
)

type remoteWorker struct {
	api.Worker
	closer jsonrpc.ClientCloser
}

func (r *remoteWorker) NewSector(ctx context.Context, sector abi.SectorID) error {
	return xerrors.New("unsupported")
}

func connectRemoteWorker(ctx context.Context, fa api.Common, url string) (*remoteWorker, error) {
	token, err := fa.AuthNew(ctx, []auth.Permission{"admin"})
	if err != nil {
		return nil, xerrors.Errorf("creating auth token for remote connection: %w", err)
	}

	headers := http.Header{}
	headers.Add("Authorization", "Bearer "+string(token))

	wapi, closer, err := client.NewWorkerRPCV0(context.TODO(), url, headers)
	if err != nil {
		return nil, xerrors.Errorf("creating jsonrpc client: %w", err)
	}

	wver, err := wapi.Version(ctx)
	if err != nil {
		closer()
		return nil, err
	}

	if !wver.EqMajorMinor(api.WorkerAPIVersion0) {
		return nil, xerrors.Errorf("unsupported worker api version: %s (expected %s)", wver, api.WorkerAPIVersion0)
	}
	//yungojs
	ip := strings.Split(url, "/")[2]
	wc := sealer.WConfig{
		ip,
		0,
	}
	//conf := sectorstorage.GetPledgeConfig()
	types,err := wapi.TaskTypes(ctx)
	if err != nil {
		return nil, err
	}
	if _,ok := types[sealtasks.TTPreCommit1];ok{
		wc.P1count = 7
	}
	conf := sealer.GetPledgeConfig()
	for _, v := range conf.WorkerConfigs {
		if v.Ip == ip {
			return &remoteWorker{wapi, closer}, nil
		}
	}
	conf.WorkerConfigs = append(conf.WorkerConfigs, wc)
	//yungojs
	return &remoteWorker{wapi, closer}, conf.SaveConfigFile()
}

func (r *remoteWorker) Close() error {
	r.closer()
	return nil
}

var _ sealer.Worker = &remoteWorker{}
