package main

import (
	"os"

	kvstore "github.com/pingcap-incubator/tinykv/store"

	"github.com/pingcap-incubator/tinykv/client/config"
	"github.com/pingcap-incubator/tinykv/client/logger"
	"github.com/pingcap-incubator/tinykv/client/memdb"
	"github.com/pingcap-incubator/tinykv/client/server"
	"github.com/pingcap-incubator/tinykv/store/mockstore"
	"github.com/pingcap-incubator/tinykv/store/tikv"
	"github.com/pingcap/tidb/parser/terror"
)

func init() {
	// Register commands
	memdb.RegisterKeyCommands()
	memdb.RegisterStringCommands()
	memdb.RegisterListCommands()
	memdb.RegisterSetCommands()
	memdb.RegisterHashCommands()
	memdb.RegisterPubSubCommands()
	memdb.RegisterSortedSetCommands()
	memdb.RegisterStreamCommands()
}
func registerStores() {
	err := kvstore.Register("tikv", tikv.Driver{})
	terror.MustNil(err)
	err = kvstore.Register("mocktikv", mockstore.MockDriver{})
	terror.MustNil(err)
}
func main() {
	// setup config
	cfg, err := config.Setup()
	if err != nil {
		logger.Error(err)
		os.Exit(1)
	}
	registerStores()
	// setup logger
	err = logger.SetUp(cfg)
	if err != nil {
		logger.Error(err)
		os.Exit(1)
	}
	// setup tcp server
	err = server.Start(cfg)
	if err != nil {
		logger.Error(err)
		os.Exit(1)
	}
}
