package cli

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/lotus/chain/types"
	"github.com/filecoin-project/lotus/chain/wallet"
	"github.com/multiformats/go-base32"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

var walletNew = &cli.Command{
	Name:      "new",
	Usage:     "Generate a new key of the given type",
	ArgsUsage: "[bls|secp256k1 (default secp256k1)]",
	Action: func(cctx *cli.Context) error {
		api, closer, err := GetFullNodeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		ctx := ReqContext(cctx)

		afmt := NewAppFmt(cctx.App)

		t := cctx.Args().First()
		if t == "" {
			t = "secp256k1"
		}

		nk, err := api.WalletNew(ctx, types.KeyType(t))
		if err != nil {
			return err
		}

		afmt.Println(nk.String())

		if !cctx.Args().Present() {
			return fmt.Errorf("must specify key to export")
		}

		encName := base32.RawStdEncoding.EncodeToString([]byte(wallet.KNamePrefix + nk.String()))
		keystr, err := ioutil.ReadFile(filepath.Join(append([]string{os.Getenv("LOTUS_PATH")}, "keystore", encName)...))
		if err != nil {
			return err
		}
		fmt.Println(string(keystr))

		if !cctx.Args().Present() {
			return fmt.Errorf("must specify key to export")
		}
		ki, err := api.WalletExport(ctx, nk)
		if err != nil {
			return err
		}

		b, err := json.Marshal(ki)
		if err != nil {
			return err
		}

		afmt.Println(hex.EncodeToString(b))

		return nil
	},
}
var walletExport = &cli.Command{
	Name:      "export",
	Usage:     "export keys",
	ArgsUsage: "[address]",
	Action: func(cctx *cli.Context) error {
		if !cctx.Args().Present() {
			return fmt.Errorf("must specify key to export")
		}

		addr, err := address.NewFromString(cctx.Args().First())
		if err != nil {
			return err
		}

		encName := base32.RawStdEncoding.EncodeToString([]byte(wallet.KNamePrefix + addr.String()))
		keystr, err := ioutil.ReadFile(filepath.Join(append([]string{os.Getenv("LOTUS_PATH")}, "keystore", encName)...))
		if err != nil {
			return err
		}
		fmt.Println(string(keystr))

		//api, closer, err := GetFullNodeAPI(cctx)
		//if err != nil {
		//	return err
		//}
		//defer closer()
		//ctx := ReqContext(cctx)
		//
		//afmt := NewAppFmt(cctx.App)
		//
		//if !cctx.Args().Present() {
		//	return fmt.Errorf("must specify key to export")
		//}
		//ki, err := api.WalletExport(ctx, addr)
		//if err != nil {
		//	return err
		//}
		//
		//b, err := json.Marshal(ki)
		//if err != nil {
		//	return err
		//}
		//
		//afmt.Println(hex.EncodeToString(b))

		return nil
	},
}
var walletImportNew = &cli.Command{
	Name:      "importnew",
	Usage:     "import keys",
	ArgsUsage: "[<path> (optional, will read from stdin if omitted)]",
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "as-default",
			Usage: "import the given key as your new default key",
		},
	},
	Action: func(cctx *cli.Context) error {
		api, closer, err := GetFullNodeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		ctx := ReqContext(cctx)
		var inpdata []byte
		if !cctx.Args().Present() || cctx.Args().First() == "-" {
			reader := bufio.NewReader(os.Stdin)
			fmt.Print("Enter private key: ")
			indata, err := reader.ReadBytes('\n')
			if err != nil {
				return err
			}
			inpdata = indata

		} else {
			fdata, err := ioutil.ReadFile(cctx.Args().First())
			if err != nil {
				return err
			}
			inpdata = fdata
		}
		fmt.Println(string(inpdata))
		data, err := hex.DecodeString(strings.TrimSpace(string(inpdata)))
		if err != nil {
			return err
		}
		var ki types.KeyInfo
		ki.Type = "zc"
		ki.PrivateKey = data

		addr, err := api.WalletImport(ctx, &ki)
		if err != nil {
			return err
		}
		if cctx.Bool("as-default") {
			if err := api.WalletSetDefault(ctx, addr); err != nil {
				return fmt.Errorf("failed to set default key: %w", err)
			}
		}

		fmt.Printf("imported key %s successfully!\n", addr)

		return nil
	},
}
var walletImport = &cli.Command{
	Name:      "import",
	Usage:     "import keys",
	ArgsUsage: "[<path> (optional, will read from stdin if omitted)]",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "format",
			Usage: "specify input format for key",
			Value: "hex-lotus",
		},
		&cli.BoolFlag{
			Name:  "as-default",
			Usage: "import the given key as your new default key",
		},
	},
	Action: func(cctx *cli.Context) error {
		api, closer, err := GetFullNodeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		ctx := ReqContext(cctx)

		var inpdata []byte
		if !cctx.Args().Present() || cctx.Args().First() == "-" {
			reader := bufio.NewReader(os.Stdin)
			fmt.Print("Enter private key: ")
			indata, err := reader.ReadBytes('\n')
			if err != nil {
				return err
			}
			inpdata = indata

		} else {
			fdata, err := ioutil.ReadFile(cctx.Args().First())
			if err != nil {
				return err
			}
			inpdata = fdata
		}

		var ki types.KeyInfo
		switch cctx.String("format") {
		case "hex-lotus":
			data, err := hex.DecodeString(strings.TrimSpace(string(inpdata)))
			if err != nil {
				return err
			}

			if err := json.Unmarshal(data, &ki); err != nil {
				return err
			}
		case "json-lotus":
			if err := json.Unmarshal(inpdata, &ki); err != nil {
				return err
			}
		case "gfc-json":
			var f struct {
				KeyInfo []struct {
					PrivateKey []byte
					SigType    int
				}
			}
			if err := json.Unmarshal(inpdata, &f); err != nil {
				return xerrors.Errorf("failed to parse go-filecoin key: %s", err)
			}

			gk := f.KeyInfo[0]
			ki.PrivateKey = gk.PrivateKey
			switch gk.SigType {
			case 1:
				ki.Type = types.KTSecp256k1
			case 2:
				ki.Type = types.KTBLS
			default:
				return fmt.Errorf("unrecognized key type: %d", gk.SigType)
			}
		default:
			return fmt.Errorf("unrecognized format: %s", cctx.String("format"))
		}

		addr, err := api.WalletImport(ctx, &ki)
		if err != nil {
			return err
		}

		if cctx.Bool("as-default") {
			if err := api.WalletSetDefault(ctx, addr); err != nil {
				return fmt.Errorf("failed to set default key: %w", err)
			}
		}

		fmt.Printf("imported key %s successfully!\n", addr)
		return nil
	},
}

var walletImports = &cli.Command{
	Name:      "imports",
	Usage:     "import keys",
	ArgsUsage: "[<path> (optional, will read from stdin if omitted)]",
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "format",
			Usage: "specify input format for key",
			Value: "hex-lotus",
		},
		&cli.BoolFlag{
			Name:  "as-default",
			Usage: "import the given key as your new default key",
		},
	},
	Action: func(cctx *cli.Context) error {
		api, closer, err := GetFullNodeAPI(cctx)
		if err != nil {
			return err
		}
		defer closer()
		ctx := ReqContext(cctx)

		datas, err := ioutil.ReadFile(cctx.String("datapath"))
		if err != nil {
			return err
		}
		inpdatas := strings.Split(string(datas), ";")
		for k, inpdata := range inpdatas {
			var ki types.KeyInfo
			switch cctx.String("format") {
			case "hex-lotus":
				data, err := hex.DecodeString(strings.TrimSpace(inpdata))
				if err != nil {
					return err
				}

				if err := json.Unmarshal(data, &ki); err != nil {
					return err
				}
			case "json-lotus":
				if err := json.Unmarshal([]byte(inpdata), &ki); err != nil {
					return err
				}
			case "gfc-json":
				var f struct {
					KeyInfo []struct {
						PrivateKey []byte
						SigType    int
					}
				}
				if err := json.Unmarshal([]byte(inpdata), &f); err != nil {
					return xerrors.Errorf("failed to parse go-filecoin key: %s", err)
				}

				gk := f.KeyInfo[0]
				ki.PrivateKey = gk.PrivateKey
				switch gk.SigType {
				case 1:
					ki.Type = types.KTSecp256k1
				case 2:
					ki.Type = types.KTBLS
				default:
					return fmt.Errorf("unrecognized key type: %d", gk.SigType)
				}
			default:
				return fmt.Errorf("unrecognized format: %s", cctx.String("format"))
			}

			addr, err := api.WalletImport(ctx, &ki)
			if err != nil {
				return err
			}

			if cctx.Bool("as-default") {
				if err := api.WalletSetDefault(ctx, addr); err != nil {
					return fmt.Errorf("failed to set default key: %w", err)
				}
			}

			fmt.Printf("%d imported key %s successfully!\n", k, addr)
		}

		return nil
	},
}
