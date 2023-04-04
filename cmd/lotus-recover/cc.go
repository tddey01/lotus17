package main

import (
	"github.com/docker/go-units"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/lotus/storage/sealer/ffiwrapper"
	"github.com/filecoin-project/lotus/storage/sealer/ffiwrapper/basicfs"
	"github.com/mitchellh/go-homedir"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
	"os"
)

var sealCcCmd = &cli.Command{
	Name:  "cc",
	Usage: "Benchmark seal and winning post and window post",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "storage-dir",
			Value: ".lotus-recover",
			Usage: "path to the storage directory that will store sectors long term",
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
			Value: 1,
		},
	},
	Action: func(c *cli.Context) error {
		if c.Bool("no-gpu") {
			err := os.Setenv("BELLMAN_NO_GPU", "1")
			if err != nil {
				return xerrors.Errorf("setting no-gpu flag: %w", err)
			}
		}

		if c.String("miner-addr") == "" {
			return xerrors.Errorf("矿工号不能为空！")
		}

		var sbdir string

		sdir, err := homedir.Expand(c.String("storage-dir"))
		if err != nil {
			return err
		}

		err = os.MkdirAll(sdir, 0775) //nolint:gosec
		if err != nil {
			return xerrors.Errorf("creating sectorbuilder dir: %w", err)
		}

		//// TODO: pretty sure this isnt even needed?

		sbdir = sdir

		// miner address
		maddr, err := address.NewFromString(c.String("miner-addr"))
		if err != nil {
			return err
		}
		amid, err := address.IDFromAddress(maddr)
		if err != nil {
			return err
		}
		mid := abi.ActorID(amid)

		// sector size
		sectorSizeInt, err := units.RAMInBytes(c.String("sector-size"))
		if err != nil {
			return err
		}
		sectorSize := abi.SectorSize(sectorSizeInt)

		sbfs := &basicfs.Provider{
			Root: sbdir,
		}

		sb, err := ffiwrapper.New(sbfs)
		if err != nil {
			return err
		}

		err = runSeals(sb, sdir, c.Int("parallel"), mid, sectorSize)
		if err != nil {
			return xerrors.Errorf("failed to run seals: %w", err)
		}

		return nil
	},
}
