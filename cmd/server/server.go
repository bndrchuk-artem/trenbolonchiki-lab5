package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/bndrchuk-artem/trenbolonchiki-lab5/httptools"
	"github.com/bndrchuk-artem/trenbolonchiki-lab5/signal"
)

const confResponseDelaySec = "CONF_RESPONSE_DELAY_SEC"
const confHealthFailure = "CONF_HEALTH_FAILURE"
const teamName = "trenbolonchiki"

var port = flag.Int("port", 8080, "server port")
var dbHost = flag.String("db-host", "db:8082", "database host:port")

type Response struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func main() {
	flag.Parse()

	if err := initializeTeamData(); err != nil {
		log.Printf("Failed to initialize team data: %v", err)
	}

	h := new(http.ServeMux)

	h.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "text/plain")
		if failConfig := os.Getenv(confHealthFailure); failConfig == "true" {
			rw.WriteHeader(http.StatusInternalServerError)
			_, _ = rw.Write([]byte("FAILURE"))
		} else {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("OK"))
		}
	})

	report := make(Report)

	h.HandleFunc("/api/v1/some-data", func(rw http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			key = teamName
		}

		dbResp, err := http.Get(fmt.Sprintf("http://%s/db/%s", *dbHost, key))
		if err != nil {
			log.Printf("Failed to fetch from DB: %v", err)
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}
		defer dbResp.Body.Close()

		if dbResp.StatusCode == http.StatusNotFound {
			rw.WriteHeader(http.StatusNotFound)
			return
		}

		var dbData Response
		if err := json.NewDecoder(dbResp.Body).Decode(&dbData); err != nil {
			log.Printf("Failed to decode DB response: %v", err)
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}

		respDelayString := os.Getenv(confResponseDelaySec)
		if delaySec, parseErr := time.ParseDuration(respDelayString + "s"); parseErr == nil && delaySec > 0 {
			time.Sleep(delaySec)
		}

		report.Process(r)

		rw.Header().Set("content-type", "application/json")
		rw.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(rw).Encode(map[string]string{
			"key":   dbData.Key,
			"value": dbData.Value,
		})
	})

	h.Handle("/report", report)

	server := httptools.CreateServer(*port, h)
	server.Start()
	signal.WaitForTerminationSignal()
}

func initializeTeamData() error {
	currentDate := time.Now().Format("2006-01-02")

	payload := map[string]interface{}{
		"value": currentDate,
	}

	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	dbURL := fmt.Sprintf("http://%s/db/%s", *dbHost, teamName)
	resp, err := http.Post(dbURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to post to DB: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("DB returned status %d: %s", resp.StatusCode, string(body))
	}

	log.Printf("Successfully initialized team data for '%s' with date: %s", teamName, currentDate)
	return nil
}
