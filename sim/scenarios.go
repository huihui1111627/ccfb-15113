package sim

import "fmt"

// Catalog returns the full deterministic scenario suite: baseline runs plus
// loss/reorder/partition/crash/rejoin combinations, each with a fixed seed.
func Catalog() []Scenario {
	scenarios := []Scenario{}
	add := func(s Scenario) {
		s.Name = fmt.Sprintf("%02d-%s", len(scenarios)+1, s.Name)
		scenarios = append(scenarios, s)
	}

	// 1-4. Clean baselines at several group sizes.
	for i, n := range []int{2, 3, 4, 6} {
		add(Scenario{
			Name:      fmt.Sprintf("baseline-clean-N%d", n),
			Seed:      1000 + int64(i),
			Members:   n,
			MinDelay:  2, MaxDelay: 5,
			AppTraffic: trafficAll(n, 14, 6),
			Events:     stopAt(120),
		})
	}

	// 5-8. Uniform loss sweeps.
	for i, loss := range []float64{0.1, 0.2, 0.35, 0.5} {
		add(Scenario{
			Name:      fmt.Sprintf("uniform-loss-%dpct", int(loss*100)),
			Seed:      2000 + int64(i),
			Members:   4,
			BaseLoss:  loss,
			MinDelay:  2, MaxDelay: 7,
			AppTraffic: trafficAll(4, 12, 7),
			Events:     stopAt(150),
		})
	}
	// 9-12. Same losses with wide delay jitter (stronger reordering).
	for i, loss := range []float64{0.1, 0.2, 0.35, 0.5} {
		add(Scenario{
			Name:      fmt.Sprintf("loss-%dpct-wide-jitter", int(loss*100)),
			Seed:      2100 + int64(i),
			Members:   5,
			BaseLoss:  loss,
			MinDelay:  1, MaxDelay: 16,
			AppTraffic: trafficAll(5, 10, 8),
			Events:     stopAt(180),
		})
	}

	// 13-16. Single member crashes mid-run, no rejoin.
	for i, at := range []int64{40, 55, 70, 30} {
		add(Scenario{
			Name:      fmt.Sprintf("crash-m%d-no-rejoin", 2+i%3),
			Seed:      3000 + int64(i),
			Members:   4,
			BaseLoss:  0.15,
			MinDelay:  2, MaxDelay: 9,
			AppTraffic: trafficAll(4, 14, 7),
			Events: append(stopAt(220),
				ScenarioEvent{At: at, Kind: EvCrash, Member: 2 + i%3},
				ScenarioEvent{At: at + 2, Kind: EvAppSend, Member: 1, Text: "after-crash"},
			),
		})
	}

	// 17-20. Crash followed by rejoin as a fresh incarnation.
	for i := 0; i < 4; i++ {
		at := int64(35 + i*12)
		add(Scenario{
			Name:      "crash-rejoin",
			Seed:      3100 + int64(i),
			Members:   4,
			BaseLoss:  0.2,
			MinDelay:  2, MaxDelay: 10,
			AppTraffic: trafficAll(4, 16, 7),
			Events: append(stopAt(260),
				ScenarioEvent{At: at, Kind: EvCrash, Member: 3},
				ScenarioEvent{At: at + 45, Kind: EvRejoin, Member: 3},
				ScenarioEvent{At: at + 55, Kind: EvAppSend, Member: 1, Text: "post-rejoin"},
			),
		})
	}

	// 21-23. Two sequential crashes, one rejoin.
	for i := 0; i < 3; i++ {
		add(Scenario{
			Name:      "double-crash",
			Seed:      3200 + int64(i),
			Members:   5,
			BaseLoss:  0.25,
			MinDelay:  2, MaxDelay: 12,
			AppTraffic: trafficAll(5, 14, 8),
			Events: append(stopAt(300),
				ScenarioEvent{At: 40, Kind: EvCrash, Member: 2},
				ScenarioEvent{At: 75, Kind: EvCrash, Member: 4},
				ScenarioEvent{At: 120, Kind: EvRejoin, Member: 2},
			),
		})
	}

	// 24-27. Network partitions that heal.
	for i, m := range []int{1, 2, 3, 4} {
		add(Scenario{
			Name:      fmt.Sprintf("partition-m%d-heals", m),
			Seed:      4000 + int64(i),
			Members:   4,
			BaseLoss:  0.1,
			MinDelay:  2, MaxDelay: 8,
			AppTraffic: trafficAll(4, 16, 7),
			Events: append(stopAt(260),
				ScenarioEvent{At: 45, Kind: EvPartition, Member: m},
				ScenarioEvent{At: 110, Kind: EvHeal, Member: m},
			),
		})
	}

	// 28-29. Partition then crash inside the minority side; coordinator
	// partition exercising leader takeover.
	add(Scenario{
		Name: "partition-then-crash", Seed: 4100, Members: 5,
		BaseLoss: 0.15, MinDelay: 2, MaxDelay: 9,
		AppTraffic: trafficAll(5, 15, 8),
		Events: append(stopAt(300),
			ScenarioEvent{At: 40, Kind: EvPartition, Member: 5},
			ScenarioEvent{At: 80, Kind: EvCrash, Member: 5},
			ScenarioEvent{At: 130, Kind: EvRejoin, Member: 5},
		),
	})
	add(Scenario{
		Name: "coordinator-partition", Seed: 4101, Members: 4,
		BaseLoss: 0.2, MinDelay: 2, MaxDelay: 10,
		AppTraffic: trafficAll(4, 16, 7),
		Events: append(stopAt(280),
			ScenarioEvent{At: 50, Kind: EvPartition, Member: 1},
			ScenarioEvent{At: 130, Kind: EvHeal, Member: 1},
		),
	})

	// 30-32. Global loss bursts.
	for i := 0; i < 3; i++ {
		add(Scenario{
			Name:      "loss-burst",
			Seed:      5000 + int64(i),
			Members:   4,
			BaseLoss:  0.05,
			MinDelay:  2, MaxDelay: 8,
			AppTraffic: trafficAll(4, 18, 6),
			Events: append(stopAt(240),
				ScenarioEvent{At: 40, Kind: EvGlobalLoss, Value: 0.55},
				ScenarioEvent{At: 90, Kind: EvGlobalLoss, Value: 0},
			),
		})
	}

	// 33-35. One badly degraded directed link (asymmetric).
	for i, pair := range [][2]int{{1, 3}, {2, 4}, {3, 1}} {
		add(Scenario{
			Name:      fmt.Sprintf("bad-link-m%d-m%d", pair[0], pair[1]),
			Seed:      5100 + int64(i),
			Members:   4,
			BaseLoss:  0.05,
			MinDelay:  2, MaxDelay: 8,
			AppTraffic: trafficAll(4, 16, 7),
			Events: append(stopAt(240),
				ScenarioEvent{At: 30, Kind: EvLinkLoss, Member: pair[0], Member2: pair[1], Value: 0.6},
				ScenarioEvent{At: 120, Kind: EvLinkClear, Member: pair[0], Member2: pair[1]},
			),
		})
	}

	// 36. Kitchen-sink: loss + partition + crash + rejoin + burst.
	add(Scenario{
		Name: "kitchen-sink", Seed: 9000, Members: 6,
		BaseLoss: 0.2, MinDelay: 1, MaxDelay: 14,
		AppTraffic: trafficAll(6, 20, 9),
		Events: append(stopAt(380),
			ScenarioEvent{At: 35, Kind: EvGlobalLoss, Value: 0.35},
			ScenarioEvent{At: 60, Kind: EvCrash, Member: 3},
			ScenarioEvent{At: 75, Kind: EvPartition, Member: 5},
			ScenarioEvent{At: 120, Kind: EvGlobalLoss, Value: 0},
			ScenarioEvent{At: 140, Kind: EvHeal, Member: 5},
			ScenarioEvent{At: 160, Kind: EvRejoin, Member: 3},
			ScenarioEvent{At: 200, Kind: EvCrash, Member: 2},
			ScenarioEvent{At: 260, Kind: EvRejoin, Member: 2},
			ScenarioEvent{At: 210, Kind: EvAppSend, Member: 1, Text: "sink-msg"},
		),
	})

	return scenarios
}

func trafficAll(n int, count int, period int64) []AppTrafficSpec {
	out := make([]AppTrafficSpec, n)
	for i := range out {
		out[i] = AppTrafficSpec{Member: i + 1, Period: period, Count: count}
	}
	return out
}

func stopAt(t int64) []ScenarioEvent {
	return []ScenarioEvent{{At: t, Kind: EvStop}}
}
