package web

import (
	"embed"
	"encoding/json"
	"fmt"
	"net/http"

	"kitecomm/sim"
)

//go:embed static/index.html
var staticFS embed.FS

// Serve starts the visualization HTTP server.
func Serve(addr string) {
	catalog := sim.Catalog()
	byName := map[string]sim.Scenario{}
	for _, sc := range catalog {
		byName[sc.Name] = sc
	}

	mux := http.NewServeMux()

	mux.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		data, err := staticFS.ReadFile("static/index.html")
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(data)
	})

	mux.HandleFunc("GET /api/scenarios", func(w http.ResponseWriter, r *http.Request) {
		names := make([]string, len(catalog))
		for i, sc := range catalog {
			names[i] = sc.Name
		}
		writeJSON(w, names)
	})

	mux.HandleFunc("GET /api/snapshot", func(w http.ResponseWriter, r *http.Request) {
		name := r.URL.Query().Get("scenario")
		sc, ok := byName[name]
		if !ok {
			sc = catalog[0]
		}
		writeJSON(w, BuildSnapshot(sc))
	})

	fmt.Printf("kitecomm visualization on http://localhost%s\n", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		fmt.Printf("server error: %v\n", err)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
