package metadata

import (
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

const (
	// DefaultVirtualNodes is the default number of virtual nodes per physical node
	// More virtual nodes = better distribution but more memory
	DefaultVirtualNodes = 150
)

// ConsistentHash implements consistent hashing for partition assignment
type ConsistentHash struct {
	virtualNodes int
	ring         []uint32          // Sorted hash values
	nodeMap      map[uint32]string // hash -> node address
	nodes        map[string]bool   // Set of physical nodes
	mu           sync.RWMutex
}

// NewConsistentHash creates a new consistent hash ring
func NewConsistentHash(virtualNodes int) *ConsistentHash {
	if virtualNodes <= 0 {
		virtualNodes = DefaultVirtualNodes
	}
	return &ConsistentHash{
		virtualNodes: virtualNodes,
		ring:         make([]uint32, 0),
		nodeMap:      make(map[uint32]string),
		nodes:        make(map[string]bool),
	}
}

// AddNode adds a node to the hash ring
func (ch *ConsistentHash) AddNode(nodeAddr string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.nodes[nodeAddr] {
		return // Already exists
	}

	ch.nodes[nodeAddr] = true

	// Add virtual nodes
	for i := 0; i < ch.virtualNodes; i++ {
		hash := ch.hash(fmt.Sprintf("%s#%d", nodeAddr, i))
		ch.ring = append(ch.ring, hash)
		ch.nodeMap[hash] = nodeAddr
	}

	// Sort the ring
	sort.Slice(ch.ring, func(i, j int) bool {
		return ch.ring[i] < ch.ring[j]
	})
}

// RemoveNode removes a node from the hash ring
func (ch *ConsistentHash) RemoveNode(nodeAddr string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if !ch.nodes[nodeAddr] {
		return // Doesn't exist
	}

	delete(ch.nodes, nodeAddr)

	// Remove virtual nodes
	newRing := make([]uint32, 0, len(ch.ring)-ch.virtualNodes)
	for _, hash := range ch.ring {
		if ch.nodeMap[hash] != nodeAddr {
			newRing = append(newRing, hash)
		} else {
			delete(ch.nodeMap, hash)
		}
	}
	ch.ring = newRing
}

// GetNode returns the node responsible for a given key
func (ch *ConsistentHash) GetNode(key string) string {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	if len(ch.ring) == 0 {
		return ""
	}

	hash := ch.hash(key)
	idx := ch.search(hash)
	return ch.nodeMap[ch.ring[idx]]
}

// GetNodes returns Primary and Replica nodes for a partition
func (ch *ConsistentHash) GetNodes(partitionID uint32) (primary, replica string) {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	if len(ch.ring) == 0 {
		return "", ""
	}

	// Use partition ID as key
	key := fmt.Sprintf("partition-%d", partitionID)
	hash := ch.hash(key)
	idx := ch.search(hash)

	// Primary is the first node found
	primary = ch.nodeMap[ch.ring[idx]]

	// Replica is the next different physical node
	for i := 1; i < len(ch.ring); i++ {
		nextIdx := (idx + i) % len(ch.ring)
		nextNode := ch.nodeMap[ch.ring[nextIdx]]
		if nextNode != primary {
			replica = nextNode
			break
		}
	}

	return primary, replica
}

// GetAllNodes returns all physical nodes in the ring
func (ch *ConsistentHash) GetAllNodes() []string {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	nodes := make([]string, 0, len(ch.nodes))
	for node := range ch.nodes {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	return nodes
}

// NodeCount returns the number of physical nodes
func (ch *ConsistentHash) NodeCount() int {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	return len(ch.nodes)
}

// hash computes the hash value for a key using CRC32
func (ch *ConsistentHash) hash(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// search finds the index of the first hash >= given hash
func (ch *ConsistentHash) search(hash uint32) int {
	idx := sort.Search(len(ch.ring), func(i int) bool {
		return ch.ring[i] >= hash
	})
	if idx >= len(ch.ring) {
		idx = 0 // Wrap around
	}
	return idx
}

// GetPartitionDistribution returns the number of partitions per node
// This is useful for checking balance
func (ch *ConsistentHash) GetPartitionDistribution(totalPartitions uint32) map[string]int {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	distribution := make(map[string]int)
	for node := range ch.nodes {
		distribution[node] = 0
	}

	for partitionID := uint32(0); partitionID < totalPartitions; partitionID++ {
		primary, _ := ch.GetNodes(partitionID)
		if primary != "" {
			distribution[primary]++
		}
	}

	return distribution
}
