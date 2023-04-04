package main

import (
	"github.com/filecoin-project/lotus/build"
	"github.com/filecoin-project/lotus/storage/db"
	logging "github.com/ipfs/go-log/v2"
	"github.com/urfave/cli/v2"
	"os"
)

var log = logging.Logger("lotus-recover")

func init() {
	db.NewEngine()
}
func main() {
	logging.SetLogLevel("*", "INFO")

	app := &cli.App{
		Name:    "lotus-recover",
		Usage:   "Benchmark performance of lotus on your hardware",
		Version: build.UserVersion(),
		Commands: []*cli.Command{
			sealRecoverCmd,
			storageCmd,
			delCmd,
			updateCmd,
			sectorInfo,
			storageInfo,
			releaseCmd,
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Warnf("%+v", err)
		return
	}
}
