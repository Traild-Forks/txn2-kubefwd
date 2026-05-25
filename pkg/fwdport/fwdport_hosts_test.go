package fwdport

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/txn2/kubefwd/pkg/fwdip"
	"github.com/txn2/txeh"
)

const (
	testService   = "test-svc"
	testNamespace = "default"
	testContext   = "test-ctx"
	testLocalIP   = "127.1.27.1"

	// remoteService/remoteNamespace/remoteContext are used in tests that exercise
	// multi-cluster behaviour. Values are intentionally generic.
	remoteService   = "alpha-svc"
	remoteNamespace = "team-ns"
	remoteContext   = "remote-ctx"
	remoteLocalIP   = "127.2.27.1"
)

// createTempHostsFile creates a temporary hosts file for testing
func createTempHostsFile(t *testing.T) (*HostFileWithLock, string, func()) {
	t.Helper()

	// Create a temporary directory
	tempDir, err := os.MkdirTemp("", "kubefwd-hosts-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	hostsPath := filepath.Join(tempDir, "hosts")

	// Create initial hosts file
	if err := os.WriteFile(hostsPath, []byte("127.0.0.1 localhost\n"), 0o644); err != nil {
		if removeErr := os.RemoveAll(tempDir); removeErr != nil {
			t.Logf("Warning: failed to cleanup temp dir: %v", removeErr)
		}
		t.Fatalf("Failed to create temp hosts file: %v", err)
	}

	// Create Hosts instance with temp file
	hosts, err := txeh.NewHosts(&txeh.HostsConfig{
		ReadFilePath:  hostsPath,
		WriteFilePath: hostsPath,
	})
	if err != nil {
		if removeErr := os.RemoveAll(tempDir); removeErr != nil {
			t.Logf("Warning: failed to cleanup temp dir: %v", removeErr)
		}
		t.Fatalf("Failed to create txeh.Hosts: %v", err)
	}

	hostFileWithLock := &HostFileWithLock{
		Hosts: hosts,
	}

	cleanup := func() {
		if err := os.RemoveAll(tempDir); err != nil {
			t.Logf("Warning: failed to cleanup temp dir: %v", err)
		}
	}

	return hostFileWithLock, hostsPath, cleanup
}

// createMockPortForwardOpts creates a mock PortForwardOpts for testing
func createMockPortForwardOpts(hostFile *HostFileWithLock, service, namespace, context string, localIP net.IP) *PortForwardOpts {
	return &PortForwardOpts{
		Service:    service,
		Namespace:  namespace,
		Context:    context,
		LocalIP:    localIP,
		HostFile:   hostFile,
		ClusterN:   0,
		NamespaceN: 0,
		Domain:     "",
		Hosts:      make([]string, 0),
	}
}

// TestAddHosts_SingleCall tests adding hosts in a single call
func TestAddHosts_SingleCall(t *testing.T) {
	hostFile, _, cleanup := createTempHostsFile(t)
	defer cleanup()

	localIP := net.ParseIP(testLocalIP)
	pfo := createMockPortForwardOpts(hostFile, testService, testNamespace, testContext, localIP)

	if err := pfo.AddHosts(); err != nil {
		t.Fatalf("AddHosts failed: %v", err)
	}

	// Verify hosts were added
	if len(pfo.Hosts) == 0 {
		t.Error("Expected hosts to be added, but Hosts slice is empty")
	}

	// Check that the hosts file was updated
	content := hostFile.Hosts.RenderHostsFile()
	if content == "" {
		t.Error("Expected hosts file content, got empty string")
	}

	t.Logf("Added %d hosts", len(pfo.Hosts))
}

// TestRemoveHosts_SingleCall tests removing hosts in a single call
func TestRemoveHosts_SingleCall(t *testing.T) {
	hostFile, _, cleanup := createTempHostsFile(t)
	defer cleanup()

	localIP := net.ParseIP(testLocalIP)
	pfo := createMockPortForwardOpts(hostFile, testService, testNamespace, testContext, localIP)

	// First add hosts
	if err := pfo.AddHosts(); err != nil {
		t.Fatalf("AddHosts failed: %v", err)
	}
	addedCount := len(pfo.Hosts)

	if addedCount == 0 {
		t.Fatal("No hosts were added")
	}

	// Then remove them
	pfo.removeHosts()

	// Verify the hosts slice is still populated (removeHosts doesn't clear it)
	if len(pfo.Hosts) != addedCount {
		t.Errorf("Expected %d hosts in slice after remove, got %d", addedCount, len(pfo.Hosts))
	}

	t.Logf("Removed %d hosts", addedCount)
}

// TestAddHosts_ConcurrentSameService tests concurrent AddHosts calls for the same service
func TestAddHosts_ConcurrentSameService(t *testing.T) {
	hostFile, _, cleanup := createTempHostsFile(t)
	defer cleanup()

	numGoroutines := 50
	var wg sync.WaitGroup

	localIP := net.ParseIP(testLocalIP)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pfo := createMockPortForwardOpts(hostFile, testService, testNamespace, testContext, localIP)
			_ = pfo.AddHosts()
		}()
	}

	wg.Wait()

	// All goroutines should complete without deadlock or panic
	t.Log("Concurrent AddHosts completed successfully")
}

// TestAddHosts_ConcurrentDifferentServices tests concurrent AddHosts calls for different services
func TestAddHosts_ConcurrentDifferentServices(t *testing.T) {
	hostFile, _, cleanup := createTempHostsFile(t)
	defer cleanup()

	numServices := 30
	var wg sync.WaitGroup

	for i := 0; i < numServices; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			localIP := net.ParseIP(fmt.Sprintf("127.1.27.%d", n+1))
			service := fmt.Sprintf("svc-%d", n)
			pfo := createMockPortForwardOpts(hostFile, service, testNamespace, testContext, localIP)
			_ = pfo.AddHosts()
		}(i)
	}

	wg.Wait()

	// Check that the hosts file has entries
	content := hostFile.Hosts.RenderHostsFile()
	if content == "" {
		t.Error("Expected hosts file to have content after concurrent adds")
	}

	t.Logf("Successfully added %d different services concurrently", numServices)
}

// TestRemoveHosts_Concurrent tests concurrent removeHosts calls
func TestRemoveHosts_Concurrent(t *testing.T) {
	hostFile, _, cleanup := createTempHostsFile(t)
	defer cleanup()

	// First, add multiple services
	numServices := 20
	pfos := make([]*PortForwardOpts, numServices)

	for i := 0; i < numServices; i++ {
		localIP := net.ParseIP(fmt.Sprintf("127.1.27.%d", i+1))
		service := fmt.Sprintf("svc-%d", i)
		pfo := createMockPortForwardOpts(hostFile, service, testNamespace, testContext, localIP)
		_ = pfo.AddHosts()
		pfos[i] = pfo
	}

	// Now remove them all concurrently
	var wg sync.WaitGroup
	for i := 0; i < numServices; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			pfos[n].removeHosts()
		}(i)
	}

	wg.Wait()

	t.Log("Concurrent removeHosts completed successfully")
}

// TestAddAndRemoveHosts_Concurrent tests concurrent adds and removes
func TestAddAndRemoveHosts_Concurrent(t *testing.T) {
	hostFile, _, cleanup := createTempHostsFile(t)
	defer cleanup()

	var wg sync.WaitGroup
	numOperations := 50

	// Concurrent adds
	for i := 0; i < numOperations; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			localIP := net.ParseIP(fmt.Sprintf("127.1.27.%d", (n%250)+1))
			service := fmt.Sprintf("svc-%d", n)
			pfo := createMockPortForwardOpts(hostFile, service, testNamespace, testContext, localIP)
			_ = pfo.AddHosts()
		}(i)
	}

	// Concurrent removes (some may not exist)
	for i := 0; i < numOperations; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			// Add small delay to let some adds happen first
			time.Sleep(time.Millisecond * time.Duration(n%10))

			localIP := net.ParseIP(fmt.Sprintf("127.1.27.%d", (n%250)+1))
			service := fmt.Sprintf("svc-%d", n)
			pfo := createMockPortForwardOpts(hostFile, service, testNamespace, testContext, localIP)

			// Populate Hosts slice for removal
			pfo.Hosts = []string{service, fmt.Sprintf("%s.%s", service, testNamespace)}
			pfo.removeHosts()
		}(i)
	}

	wg.Wait()

	t.Log("Concurrent adds and removes completed successfully")
}

// TestHostsFileReload_WhileWriting tests Reload() being called during concurrent writes
func TestHostsFileReload_WhileWriting(t *testing.T) {
	hostFile, _, cleanup := createTempHostsFile(t)
	defer cleanup()

	stopChan := make(chan struct{})
	var wg sync.WaitGroup

	// Goroutine continuously adding hosts
	wg.Add(1)
	go func() {
		defer wg.Done()
		counter := 0
		for {
			select {
			case <-stopChan:
				return
			default:
				localIP := net.ParseIP(fmt.Sprintf("127.1.27.%d", (counter%250)+1))
				service := fmt.Sprintf("svc-%d", counter)
				pfo := createMockPortForwardOpts(hostFile, service, testNamespace, testContext, localIP)
				_ = pfo.AddHosts()
				counter++
				time.Sleep(time.Millisecond)
			}
		}
	}()

	// Goroutine continuously reloading
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopChan:
				return
			default:
				hostFile.Lock()
				_ = hostFile.Hosts.Reload()
				hostFile.Unlock()
				time.Sleep(time.Millisecond * 5)
			}
		}
	}()

	// Let it run for a short time
	time.Sleep(100 * time.Millisecond)
	close(stopChan)
	wg.Wait()

	t.Log("Concurrent reload and write operations completed successfully")
}

// TestLockContention tests that lock contention is handled properly
func TestLockContention(t *testing.T) {
	hostFile, _, cleanup := createTempHostsFile(t)
	defer cleanup()

	numGoroutines := 100
	var wg sync.WaitGroup

	// All goroutines trying to lock at the same time
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()

			hostFile.Lock()
			// Simulate some work while holding the lock
			localIP := fmt.Sprintf("127.1.27.%d", (n%250)+1)
			hostname := fmt.Sprintf("test-%d.example.com", n)
			hostFile.Hosts.AddHost(localIP, hostname)
			time.Sleep(time.Microsecond * 100)
			if err := hostFile.Hosts.Save(); err != nil {
				t.Logf("Save error in test: %v", err)
			}
			hostFile.Unlock()
		}(i)
	}

	wg.Wait()

	t.Log("Lock contention test completed successfully")
}

// TestHostSanitization tests that illegal characters in hostnames are sanitized
func TestHostSanitization(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"simple-host", "simple-host"},
		{"host.with.dots", "host-with-dots"},
		{"host_with_underscores", "host-with-underscores"},
		{"host@with#special$chars", "host-with-special-chars"},
		{"---leading-dashes", "leading-dashes"},
		{"trailing-dashes---", "trailing-dashes"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := sanitizeHost(tt.input)
			if result != tt.expected {
				t.Errorf("sanitizeHost(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

// TestAddHost_WithSanitization tests that addHost properly handles sanitization
func TestAddHost_WithSanitization(t *testing.T) {
	hostFile, _, cleanup := createTempHostsFile(t)
	defer cleanup()

	localIP := net.ParseIP(testLocalIP)
	pfo := createMockPortForwardOpts(hostFile, "test.svc", testNamespace, testContext, localIP)

	hostFile.Lock()
	pfo.addHost("service.with.dots")
	hostFile.Unlock()

	// Should have added both the original and sanitized version
	// Original: service.with.dots
	// Sanitized: service-with-dots
	if len(pfo.Hosts) != 2 {
		t.Errorf("Expected 2 hosts (original + sanitized), got %d", len(pfo.Hosts))
	}

	found := false
	for _, host := range pfo.Hosts {
		if host == "service-with-dots" {
			found = true
			break
		}
	}

	if !found {
		t.Error("Expected sanitized hostname 'service-with-dots' to be in Hosts slice")
	}
}

// TestAddHosts_DifferentClusterNamespaceConfigurations tests various cluster/namespace combinations
func TestAddHosts_DifferentClusterNamespaceConfigurations(t *testing.T) {
	tests := []struct {
		name       string
		clusterN   int
		namespaceN int
		domain     string
		minHosts   int // minimum number of hosts expected
	}{
		{
			name:       "Local cluster, local namespace, no domain",
			clusterN:   0,
			namespaceN: 0,
			domain:     "",
			minHosts:   5, // service, service.ns, service.ns.svc, service.ns.svc.cluster.local, service.ns.ctx
		},
		{
			name:       "Local cluster, local namespace, with domain",
			clusterN:   0,
			namespaceN: 0,
			domain:     "example.com",
			minHosts:   7, // adds service.domain and service.ns.svc.cluster.domain
		},
		{
			name:       "Remote cluster, local namespace",
			clusterN:   1,
			namespaceN: 0,
			domain:     "",
			minHosts:   5, // service.ctx, service.ns, service.ns.svc, etc.
		},
		{
			name:       "Local cluster, remote namespace",
			clusterN:   0,
			namespaceN: 1,
			domain:     "",
			minHosts:   4, // no bare service name
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hostFile, _, cleanup := createTempHostsFile(t)
			defer cleanup()

			localIP := net.ParseIP(testLocalIP)
			pfo := createMockPortForwardOpts(hostFile, testService, testNamespace, testContext, localIP)
			pfo.ClusterN = tt.clusterN
			pfo.NamespaceN = tt.namespaceN
			pfo.Domain = tt.domain

			if err := pfo.AddHosts(); err != nil {
				t.Fatalf("AddHosts failed: %v", err)
			}

			if len(pfo.Hosts) < tt.minHosts {
				t.Errorf("Expected at least %d hosts, got %d. Hosts: %v",
					tt.minHosts, len(pfo.Hosts), pfo.Hosts)
			}
		})
	}
}

// TestConcurrentAddRemoveSameHost tests the edge case of adding and removing the same host concurrently
func TestConcurrentAddRemoveSameHost(t *testing.T) {
	hostFile, _, cleanup := createTempHostsFile(t)
	defer cleanup()

	const sameSvc = "same-svc"

	var wg sync.WaitGroup
	numOperations := 50

	localIP := net.ParseIP(testLocalIP)

	// Half the goroutines add the same service
	for i := 0; i < numOperations; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pfo := createMockPortForwardOpts(hostFile, sameSvc, testNamespace, testContext, localIP)
			_ = pfo.AddHosts()
		}()
	}

	// Other half remove the same service
	for i := 0; i < numOperations; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(time.Millisecond * 5) // slight delay to let some adds happen
			pfo := createMockPortForwardOpts(hostFile, sameSvc, testNamespace, testContext, localIP)
			pfo.Hosts = []string{
				sameSvc,
				fmt.Sprintf("%s.%s", sameSvc, testNamespace),
				fmt.Sprintf("%s.%s.%s", sameSvc, testNamespace, testContext),
			}
			pfo.removeHosts()
		}()
	}

	wg.Wait()

	// Should complete without panics or deadlocks
	t.Log("Concurrent add/remove of same host completed successfully")
}

// TestHostsFileSaveError tests handling of save errors
func TestHostsFileSaveError(t *testing.T) {
	// Create a read-only hosts file to trigger save errors
	tempDir, err := os.MkdirTemp("", "kubefwd-hosts-readonly-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer func() {
		if err := os.RemoveAll(tempDir); err != nil {
			t.Logf("Warning: failed to cleanup temp dir: %v", err)
		}
	}()

	hostsPath := filepath.Join(tempDir, "hosts")
	if err := os.WriteFile(hostsPath, []byte("127.0.0.1 localhost\n"), 0o644); err != nil {
		t.Fatalf("Failed to create temp hosts file: %v", err)
	}

	hosts, err := txeh.NewHosts(&txeh.HostsConfig{
		ReadFilePath:  hostsPath,
		WriteFilePath: hostsPath,
	})
	if err != nil {
		t.Fatalf("Failed to create txeh.Hosts: %v", err)
	}

	hostFile := &HostFileWithLock{Hosts: hosts}

	// Make file read-only
	if err := os.Chmod(hostsPath, 0o444); err != nil {
		t.Fatalf("Failed to make file read-only: %v", err)
	}

	localIP := net.ParseIP(testLocalIP)
	pfo := createMockPortForwardOpts(hostFile, testService, testNamespace, testContext, localIP)

	// This should not panic, even though save will fail
	_ = pfo.AddHosts()

	// Verify it logged the error but didn't crash
	t.Log("AddHosts with save error handled gracefully")
}

// TestHostsFileReloadError tests handling of reload errors
func TestHostsFileReloadError(t *testing.T) {
	hostFile, hostsPath, cleanup := createTempHostsFile(t)
	defer cleanup()

	// First add some hosts
	localIP := net.ParseIP(testLocalIP)
	pfo := createMockPortForwardOpts(hostFile, testService, testNamespace, testContext, localIP)
	_ = pfo.AddHosts()

	// Remove the hosts file to trigger reload error
	if err := os.Remove(hostsPath); err != nil {
		t.Fatalf("Failed to remove hosts file: %v", err)
	}

	// This should handle the reload error gracefully
	pfo.removeHosts()

	// Should not panic
	t.Log("removeHosts with reload error handled gracefully")
}

// TestRaceConditions is a placeholder test that reminds us to run with -race
func TestRaceConditions(t *testing.T) {
	t.Log("Run with: go test -race ./pkg/fwdport/... to detect race conditions")
}

func TestAddHosts_LocalDNS_RemoteCluster(t *testing.T) {
	fwdip.ResetRegistry()
	hostFile, _, cleanup := createTempHostsFile(t)
	defer cleanup()

	localIP := net.ParseIP(remoteLocalIP)
	pfo := createMockPortForwardOpts(hostFile, remoteService, remoteNamespace, remoteContext, localIP)
	pfo.ClusterN = 1
	pfo.NamespaceN = 0
	pfo.LocalDNS = true

	if err := pfo.AddHosts(); err != nil {
		t.Fatalf("AddHosts failed: %v", err)
	}

	// Should have standard K8s DNS aliases even though ClusterN > 0
	expectedHosts := []string{
		remoteService,
		remoteService + "." + remoteNamespace,
		remoteService + "." + remoteNamespace + ".svc",
		remoteService + "." + remoteNamespace + ".svc.cluster.local",
	}

	for _, expected := range expectedHosts {
		found := false
		for _, h := range pfo.Hosts {
			if h == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected host %q not found in Hosts: %v", expected, pfo.Hosts)
		}
	}

	// Should also have context-qualified entries
	contextHosts := []string{
		remoteService + "." + remoteNamespace + "." + remoteContext,
		remoteService + "." + remoteNamespace + ".svc." + remoteContext,
		remoteService + "." + remoteNamespace + ".svc.cluster." + remoteContext,
	}

	for _, expected := range contextHosts {
		found := false
		for _, h := range pfo.Hosts {
			if h == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Expected context-qualified host %q not found in Hosts: %v", expected, pfo.Hosts)
		}
	}
}

func TestAddHosts_LocalDNS_CollisionDetection(t *testing.T) {
	fwdip.ResetRegistry()
	hostFile, _, cleanup := createTempHostsFile(t)
	defer cleanup()

	const (
		collisionSvc = "redis"
		ctx1         = "ctx1"
		ctx2         = "ctx2"
		ip1          = "127.1.27.1"
		ip2          = "127.2.27.1"
	)

	// First service from context 1
	localIP1 := net.ParseIP(ip1)
	pfo1 := createMockPortForwardOpts(hostFile, collisionSvc, testNamespace, ctx1, localIP1)
	pfo1.ClusterN = 0
	pfo1.NamespaceN = 0
	pfo1.LocalDNS = true

	if err := pfo1.AddHosts(); err != nil {
		t.Fatalf("AddHosts for ctx1 failed: %v", err)
	}

	// Second service from context 2, same name+namespace
	localIP2 := net.ParseIP(ip2)
	pfo2 := createMockPortForwardOpts(hostFile, collisionSvc, testNamespace, ctx2, localIP2)
	pfo2.ClusterN = 1
	pfo2.NamespaceN = 0
	pfo2.LocalDNS = true

	if err := pfo2.AddHosts(); err != nil {
		t.Fatalf("AddHosts for ctx2 failed: %v", err)
	}

	// The bare hostname should still resolve to the first IP (first-wins)
	ip := fwdip.LookupHostnameIP(collisionSvc)
	if ip != ip1 {
		t.Errorf("Expected first-wins IP %s for %q, got %q", ip1, collisionSvc, ip)
	}

	// The second service should NOT have the bare hostname (collision)
	for _, h := range pfo2.Hosts {
		if h == collisionSvc {
			t.Errorf("Second service should not have bare %q hostname (collision)", collisionSvc)
		}
	}

	// But context-qualified entries should still be present for both
	expected := collisionSvc + "." + testNamespace + "." + ctx2
	found := false
	for _, h := range pfo2.Hosts {
		if h == expected {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected context-qualified %q in second service Hosts: %v", expected, pfo2.Hosts)
	}
}

func TestAddHosts_LocalDNS_SameIPNotCollision(t *testing.T) {
	fwdip.ResetRegistry()
	hostFile, _, cleanup := createTempHostsFile(t)
	defer cleanup()

	// Register a hostname with an IP
	fwdip.RegisterHostnameWithIP("my-svc", testLocalIP)

	// Same service re-adding with same IP should succeed
	localIP := net.ParseIP(testLocalIP)
	pfo := createMockPortForwardOpts(hostFile, "my-svc", testNamespace, testContext, localIP)
	pfo.LocalDNS = true

	if err := pfo.AddHosts(); err != nil {
		t.Fatalf("AddHosts failed: %v", err)
	}

	// "my-svc" should be in the Hosts list (not skipped)
	found := false
	for _, h := range pfo.Hosts {
		if h == "my-svc" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected 'my-svc' to be added (same IP, not a collision): %v", pfo.Hosts)
	}
}

func TestAddHosts_DefaultBehavior_RemoteClusterNoLocalDNS(t *testing.T) {
	fwdip.ResetRegistry()
	hostFile, _, cleanup := createTempHostsFile(t)
	defer cleanup()

	localIP := net.ParseIP(remoteLocalIP)
	pfo := createMockPortForwardOpts(hostFile, remoteService, remoteNamespace, remoteContext, localIP)
	pfo.ClusterN = 1
	pfo.NamespaceN = 0
	// LocalDNS defaults to false

	if err := pfo.AddHosts(); err != nil {
		t.Fatalf("AddHosts failed: %v", err)
	}

	// Should NOT have bare service name or K8s DNS aliases
	disallowed := []string{
		remoteService,
		remoteService + "." + remoteNamespace + ".svc",
		remoteService + "." + remoteNamespace + ".svc.cluster.local",
	}

	for _, bad := range disallowed {
		for _, h := range pfo.Hosts {
			if h == bad {
				t.Errorf("Host %q should NOT be present when LocalDNS is false and ClusterN > 0", bad)
			}
		}
	}

	// Should have context-qualified entry for ClusterN > 0
	expected := remoteService + "." + remoteContext
	found := false
	for _, h := range pfo.Hosts {
		if h == expected {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Expected %q for remote cluster, got: %v", expected, pfo.Hosts)
	}
}

func TestAddHost_RegistersHostnameWithIP(t *testing.T) {
	fwdip.ResetRegistry()
	hostFile, _, cleanup := createTempHostsFile(t)
	defer cleanup()

	localIP := net.ParseIP(testLocalIP)
	pfo := createMockPortForwardOpts(hostFile, testService, testNamespace, testContext, localIP)

	if err := pfo.AddHosts(); err != nil {
		t.Fatalf("AddHosts failed: %v", err)
	}

	// Verify at least the bare service name was registered with its IP
	ip := fwdip.LookupHostnameIP(testService)
	if ip != testLocalIP {
		t.Errorf("Expected IP %s for %s, got %q", testLocalIP, testService, ip)
	}

	// Verify a namespaced entry was registered with its IP
	namespacedSvc := testService + "." + testNamespace
	ip = fwdip.LookupHostnameIP(namespacedSvc)
	if ip != testLocalIP {
		t.Errorf("Expected IP %s for %s, got %q", testLocalIP, namespacedSvc, ip)
	}
}
