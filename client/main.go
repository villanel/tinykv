// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pingcap/tidb/kv"

	kvstore "github.com/pingcap-incubator/tinykv/store"
	"github.com/pingcap-incubator/tinykv/store/mockstore"
	"github.com/pingcap-incubator/tinykv/store/tikv"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/parser/terror"
	"github.com/pingcap/tidb/session"
	"github.com/pingcap/tidb/sessionctx/variable"
	"go.uber.org/zap"
)

// Flag Names
const (
	nmConfig           = "config"
	nmStore            = "store"
	nmStorePath        = "path"
	nmHost             = "host"
	nmAdvertiseAddress = "advertise-address"
	nmPort             = "P"
	nmCors             = "cors"
	nmLogLevel         = "L"
	nmLogFile          = "log-file"
	nmReportStatus     = "report-status"
	nmStatusHost       = "status-host"
	nmStatusPort       = "status"

	nmDdlLease = "lease"
)

var (
	configPath = flag.String(nmConfig, "", "config file path")

	// Base
	store            = flag.String(nmStore, "tikv", "registered store name, [tikv, mocktikv]")
	storePath        = flag.String(nmStorePath, "127.0.0.1:2379", "tidb storage path")
	host             = flag.String(nmHost, "0.0.0.0", "tidb server host")
	advertiseAddress = flag.String(nmAdvertiseAddress, "", "tidb server advertise IP")
	port             = flag.String(nmPort, "4000", "tidb server port")
	cors             = flag.String(nmCors, "", "tidb server allow cors origin")
	ddlLease         = flag.String(nmDdlLease, "45s", "schema lease duration, very dangerous to change only if you know what you do")

	// Log
	logLevel = flag.String(nmLogLevel, "info", "log level: info, debug, warn, error, fatal")
	logFile  = flag.String(nmLogFile, "", "log file path")

	// Status
	reportStatus = flagBoolean(nmReportStatus, true, "If enable status report HTTP service.")
	statusHost   = flag.String(nmStatusHost, "0.0.0.0", "tidb server status host")
	statusPort   = flag.String(nmStatusPort, "10080", "tidb server status port")
)

var (
	cfg     *config.Config
	storage kv.Storage
)

func main() {
	flag.Parse()
	registerStores()

	configWarning := loadConfig()
	overrideConfig()
	setGlobalVars()
	// If configStrict had been specified, and there had been an error, the server would already
	// have exited by now. If configWarning is not an empty string, write it to the log now that
	// it's been properly set up.
	if configWarning != "" {
		log.Warn(configWarning)
	}
	client := createStoreAndDomain()
	client.Close()

}

func registerStores() {
	err := kvstore.Register("tikv", tikv.Driver{})
	terror.MustNil(err)
	err = kvstore.Register("mocktikv", mockstore.MockDriver{})
	terror.MustNil(err)
}

func createStoreAndDomain() *tikv.RawKVClient {
	fullPath := fmt.Sprintf("%s://%s", cfg.Store, cfg.Path)
	var err error
	fmt.Print("Bootstrapping system timezone...")
	storage, err = kvstore.New(fullPath)
	if err != nil {
		log.Fatal("Failed to create storage", zap.Error(err))
	}
	s := tikv.GetTikvStore(storage)
	client, err := tikv.NewRawKVClient(s)
	if err != nil {
		log.Fatal("Failed to create storage", zap.Error(err))
	}
	client.Put([]byte("a"), []byte("a"))
	data, data2, err := client.Scan([]byte("a"), 10)
	for i := 0; i < len(data); i++ {
		fmt.Println(string(data[i]))
		fmt.Println(string(data2[i]))
	}
	client.Close()

	terror.MustNil(err)
	return client
}

// parseDuration parses lease argument string.
func parseDuration(lease string) time.Duration {
	dur, err := time.ParseDuration(lease)
	if err != nil {
		dur, err = time.ParseDuration(lease + "s")
	}
	if err != nil || dur < 0 {
		log.Fatal("invalid lease duration", zap.String("lease", lease))
	}
	return dur
}

func flagBoolean(name string, defaultVal bool, usage string) *bool {
	if !defaultVal {
		// Fix #4125, golang do not print default false value in usage, so we append it.
		usage = fmt.Sprintf("%s (default false)", usage)
		return flag.Bool(name, defaultVal, usage)
	}
	return flag.Bool(name, defaultVal, usage)
}

func loadConfig() string {
	cfg = config.GetGlobalConfig()
	if *configPath != "" {
		err := cfg.Load(*configPath)
		if err == nil {
			return ""
		}

		// Unused config item erro turns to warnings.
		if _, ok := err.(*config.ErrConfigValidationFailed); ok {
			return err.Error()
		}

		terror.MustNil(err)
	}
	return ""
}

func overrideConfig() {
	actualFlags := make(map[string]bool)
	flag.Visit(func(f *flag.Flag) {
		actualFlags[f.Name] = true
	})

	// Base
	if actualFlags[nmHost] {
		cfg.Host = *host
	}
	if actualFlags[nmAdvertiseAddress] {
		cfg.AdvertiseAddress = *advertiseAddress
	}
	if len(cfg.AdvertiseAddress) == 0 {
		cfg.AdvertiseAddress = cfg.Host
	}
	var err error
	if actualFlags[nmPort] {
		var p int
		p, err = strconv.Atoi(*port)
		terror.MustNil(err)
		cfg.Port = uint(p)
	}
	if actualFlags[nmCors] {
		fmt.Println(cors)
		cfg.Cors = *cors
	}
	if actualFlags[nmStore] {
		cfg.Store = *store
	}
	if actualFlags[nmStorePath] {
		cfg.Path = *storePath
	}
	if actualFlags[nmDdlLease] {
		cfg.Lease = *ddlLease
	}

	// Log
	if actualFlags[nmLogLevel] {
		cfg.Log.Level = *logLevel
	}
	if actualFlags[nmLogFile] {
		cfg.Log.File.Filename = *logFile
	}

	// Status
	if actualFlags[nmReportStatus] {
		cfg.Status.ReportStatus = *reportStatus
	}
	if actualFlags[nmStatusHost] {
		cfg.Status.StatusHost = *statusHost
	}
	if actualFlags[nmStatusPort] {
		var p int
		p, err = strconv.Atoi(*statusPort)
		terror.MustNil(err)
		cfg.Status.StatusPort = uint(p)
	}
}

func setGlobalVars() {
	ddlLeaseDuration := parseDuration(cfg.Lease)
	session.SetSchemaLease(ddlLeaseDuration)

	variable.SysVars[variable.Port].Value = fmt.Sprintf("%d", cfg.Port)
	variable.SysVars[variable.DataDir].Value = cfg.Path
}

func closeDomainAndStorage() {
	atomic.StoreUint32(&tikv.ShuttingDown, 1)
	err := storage.Close()
	terror.Log(errors.Trace(err))
}
