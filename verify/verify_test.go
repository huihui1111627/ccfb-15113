package verify

import (
	"fmt"
	"testing"

	"kitecomm/sim"
)

// TestCatalogGuarantees runs every deterministic scenario and asserts that all
// ordering, set-equality, void and rejoin-state guarantees hold.
func TestCatalogGuarantees(t *testing.T) {
	scenarios := sim.Catalog()
	if len(scenarios) < 30 {
		t.Fatalf("expected at least 30 scenarios, got %d", len(scenarios))
	}
	for _, sc := range scenarios {
		t.Run(sc.Name, func(t *testing.T) {
			rt := sim.NewRuntime(sc)
			rt.Run()
			rep := Run(rt)
			for _, f := range rep.Failures {
				t.Errorf("%s: %s", f.Rule, f.Detail)
			}
		})
	}
}

// TestDeterminism replays one scenario and checks the delivery count and void
// set are identical across runs (fixed seeds must give bit-stable behavior).
func TestDeterminism(t *testing.T) {
	sc := sim.Catalog()[0]
	first := runFingerprint(t, sc)
	for i := 0; i < 2; i++ {
		if got := runFingerprint(t, sc); got != first {
			t.Fatalf("non-deterministic run: %q != %q", got, first)
		}
	}
}

func runFingerprint(t *testing.T, sc sim.Scenario) string {
	t.Helper()
	rt := sim.NewRuntime(sc)
	rt.Run()
	rep := Run(rt)
	return fmtFingerprint(rep, rt)
}

func fmtFingerprint(rep Report, rt *sim.Runtime) string {
	return fmt.Sprintf("del=%d void=%d nack=%d ok=%v",
		rep.Stats.Delivered, rep.Stats.Void, rep.Stats.NackEvents, rep.OK)
}
