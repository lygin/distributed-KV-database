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
	"sort"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
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

func valid(source *core.StoreInfo, target *core.StoreInfo, region *core.RegionInfo) bool {
	return source.GetRegionSize()-target.GetRegionSize() >= 2*region.GetApproximateSize()
}

func selectSource(cluster opt.Cluster, region *core.RegionInfo, stores []*core.StoreInfo) (int, *core.RegionInfo) {
	i := len(stores) - 1
	for ; i > 0; i-- {
		var regions core.RegionsContainer
		cluster.GetPendingRegionsWithLock(stores[i].GetID(), func(rc core.RegionsContainer) { regions = rc })
		region = regions.RandomRegion(nil, nil)
		if region != nil {
			return i, region
		}
		cluster.GetFollowersWithLock(stores[i].GetID(), func(rc core.RegionsContainer) { regions = rc })
		region = regions.RandomRegion(nil, nil)
		if region != nil {
			return i, region
		}
		cluster.GetLeadersWithLock(stores[i].GetID(), func(rc core.RegionsContainer) { regions = rc })
		region = regions.RandomRegion(nil, nil)
		if region != nil {
			return i, region
		}
	}
	return -1, nil
}

func selectReplaceStore(cluster opt.Cluster, region *core.RegionInfo, stores []*core.StoreInfo, idx int) *core.StoreInfo {
	storeIds := region.GetStoreIds()
	if len(storeIds) < cluster.GetMaxReplicas() {
		return nil
	}
	for i := 0; i < idx; i++ {
		if _, ok := storeIds[stores[i].GetID()]; !ok {
			return stores[i]
		}
	}
	return nil
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	var stores []*core.StoreInfo
	var region *core.RegionInfo
	var source *core.StoreInfo
	for _, store := range cluster.GetStores() {
		if !store.IsUp() || store.DownTime() > cluster.GetMaxStoreDownTime() {
			continue
		}
		stores = append(stores, store)
	}
	sort.Slice(stores, func(i, j int) bool {
		return stores[i].GetRegionSize() < stores[j].GetRegionSize()
	})
	idx, region := selectSource(cluster, region, stores)
	if region == nil {
		return nil
	}
	source = stores[idx]
	target := selectReplaceStore(cluster, region, stores, idx)
	if target == nil {
		return nil
	}
	if !valid(source, target, region) {
		return nil
	}
	newPeer, _ := cluster.AllocPeer(target.GetID())
	opt, _ := operator.CreateMovePeerOperator(s.GetType(), cluster, region, operator.OpBalance, source.GetID(),
		target.GetID(), newPeer.GetId())
	return opt
}
