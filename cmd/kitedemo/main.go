// Command kitedemo runs the group-communication simulator either as an
// offline verification batch (`verify`) or as a web visualization server
// (`web`).
package main

import (
	"fmt"
	"os"

	"kitecomm/sim"
	"kitecomm/verify"
	"kitecomm/web"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	switch os.Args[1] {
	case "verify":
		runVerify()
	case "web":
		addr := ":8080"
		if len(os.Args) > 2 {
			addr = os.Args[2]
		}
		web.Serve(addr)
	default:
		usage()
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: kitedemo [verify|web [addr]]")
	os.Exit(2)
}

func runVerify() {
	scenarios := sim.Catalog()
	fmt.Printf("Running %d deterministic scenarios...\n\n", len(scenarios))
	allOK := true
	totalSent, totalDelivered, totalNacks := 0, 0, 0
	for _, sc := range scenarios {
		rt := sim.NewRuntime(sc)
		rt.Run()
		rep := verify.Run(rt)
		totalSent += rep.Stats.Sent
		totalDelivered += rep.Stats.Delivered
		totalNacks += rep.Stats.NackEvents
		status := "PASS"
		if !rep.OK {
			status = "FAIL"
			allOK = false
		}
		fmt.Printf("[%s] %-34s sent=%-4d delivered=%-4d void=%-3d views=%-2d nacks=%-4d\n",
			status, sc.Name, rep.Stats.Sent, rep.Stats.Delivered,
			rep.Stats.Void, rep.Stats.Views, rep.Stats.NackEvents)
		for _, f := range rep.Failures {
			fmt.Printf("        ! %-12s %s\n", f.Rule, f.Detail)
		}
	}
	fmt.Println()
	fmt.Printf("totals: sent=%d delivered=%d nack=%d\n", totalSent, totalDelivered, totalNacks)
	if allOK {
		fmt.Println("ALL CHECKS PASSED")
	} else {
		fmt.Println("SOME CHECKS FAILED")
		os.Exit(1)
	}
}
