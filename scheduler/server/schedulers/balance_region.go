// Copyright 2017 PingCAP, Inc.
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

package schedulers

import (
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

type storeWithSort []*core.StoreInfo

func (s storeWithSort) Len() int {
	return  len(s)
}

func (s storeWithSort) Less(i, j int) bool {
	return s[i].GetRegionSize()<s[j].GetRegionSize()
}

func (s storeWithSort) Swap(i, j int) {
	s[i],s[j]=s[j],s[i]
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	 stores := make(storeWithSort, 0)
	maxTime := cluster.GetMaxStoreDownTime()
	for _, store := range cluster.GetStores() {
		if store.IsUp()&&store.DownTime()<maxTime{
			stores = append(stores, store)
		}
	}
	sort.Sort(stores)
   i:=len(stores)-1
   if i<2{
   	return nil
   }
    isLeader :=false
   var resRegion *core.RegionInfo
   start,end:=[]byte(""),[]byte("")
	for  i > 0 {
		cluster.GetPendingRegionsWithLock(stores[i].GetID(), func(rc core.RegionsContainer) {
		resRegion = rc.RandomRegion(start, end)})
		if resRegion != nil {
			break
		}
		cluster.GetFollowersWithLock(stores[i].GetID(), func(rc core.RegionsContainer) {
		resRegion = rc.RandomRegion(start, end)})
		if resRegion != nil {
			break
		}
		cluster.GetLeadersWithLock(stores[i].GetID(), func(rc core.RegionsContainer) {
		resRegion = rc.RandomRegion(start, end)})
		if resRegion != nil {
			isLeader=true
			break
		}
		i--
	}
 if resRegion==nil{
 	return nil
 }
 src:=stores[i]
 var dst *core.StoreInfo
storesId:=resRegion.GetStoreIds()

if len(storesId)<cluster.GetMaxReplicas(){
	return nil
}
//从前向后取得合适store
for idx:=0;idx<i;idx++{
	if _, ok := storesId[stores[idx].GetID()]; !ok {
		dst = stores[idx]
		break
	}
}
if dst==nil{
	return  nil
}
if src.GetRegionSize()-dst.GetRegionSize()>2*resRegion.GetApproximateSize(){
	peer, err := cluster.AllocPeer(dst.GetID())
	if err!=nil {
		return nil
	}
	kind:=operator.OpBalance
	if isLeader {
		kind |= operator.OpLeader
	}
	op, err := operator.CreateMovePeerOperator("", cluster, resRegion, kind, src.GetID(), dst.GetID(), peer.GetId())
	if err!=nil {
		return nil
	}
	return op
}
return nil
}
