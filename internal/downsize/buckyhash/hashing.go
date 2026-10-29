package hashing

import (
	"crypto/md5"
	"encoding/json"
	"fmt"

	// "log"
	// "os"
	"strconv"
	"strings"
)

// Node is a server and instance value used in the hash ring.  A key is
// mapped to one or more of the configured Node structs in the hash ring.
type Node struct {
	Server   string
	Port     int
	Instance string
}

// JSONRingType is a datastructure that identifies the name of the server
// buckdy is running on and contains a slice of nodes which are
// "server:instance" (where ":instance" is optional) formatted strings
type JSONRingType struct {
	Name     string
	Nodes    []Node
	Algo     string
	Replicas int
}

// HashRing is an interface that allows us to plug in multiple hash ring
// implementations.
type HashRing interface {

	// Len returns the number of Nodes or servers in the hash ring.
	Len() int

	// GetNode returns a Node after using the hashing algorithm on the
	// provided key.
	GetNode(key string) Node

	// GetNodes is similar to GetNode but returns a slice of Nodes who's
	// length is the smaller of the number of nodes in the ring or
	// the replication factor.
	GetNodes(key string) []Node

	// AddNode adds a new Node to the hash ring.  This should not be used
	// after you have begun calling GetNode or GetNodes.
	AddNode(node Node)

	// Replicas returns the number of replicas the hash ring is configured
	// for.  This is the number of shards that each key should be stored
	// on.
	Replicas() int

	// Nodes returns a slice of Node detailing all the servers in the hash
	// ring.
	Nodes() []Node
}

// RingEntry is used to record the position of Nodes in the ring.  Not used
// in all implementations.
type RingEntry struct {
	position int
	node     Node
}

// CarbonHashRing represents Graphite's carbon-cache.py hashing algorithm.
type CarbonHashRing struct {
	ring     []RingEntry
	nodes    []Node
	replicas int
}

// String marshals a JSONRingType into its string representation
func (j *JSONRingType) String() string {
	blob, err := json.Marshal(j)
	if err != nil {
		return err.Error()
	}
	return string(blob)
}

// NewCarbonHashRing sets up a new CarbonHashRing and returns it.
func NewCarbonHashRing() *CarbonHashRing {
	var chr = new(CarbonHashRing)
	chr.ring = make([]RingEntry, 0, 10)
	chr.nodes = make([]Node, 0, 10)
	chr.replicas = 100

	return chr
}

// NewNode returns a node object setup with the given server string and
// instance string.  None or empty instances should be represented by ""
func NewNode(server string, port int, instance string) (n Node) {
	n.Server = server
	n.Port = port
	n.Instance = instance
	return n
}

// NewNodeParser parses a HOST[:PORT][=INSTANCE] format string and builds a
// Node object which is returned.  An error is returned if the string could
// not be parsed.
func NewNodeParser(s string) (Node, error) {
	var (
		state    int
		hostname []rune
		port     []rune
		instance []rune

		parsedPort int64
		err        error
	)

	for _, v := range s {
		switch state {
		case 0:
			// server name
			switch v {
			case ':':
				state = 1
			case '=':
				state = 2
			default:
				hostname = append(hostname, v)
			}
		case 1:
			// server:port
			switch v {
			case '=':
				state = 2
			case ':':
				return Node{}, fmt.Errorf("Error parsing port in %s", s)
			default:
				port = append(port, v)
			}
		case 2:
			// server:port:instance
			instance = append(instance, v)
		default:
			panic("FSM parsing failure")
		}
	}

	if len(port) > 0 {
		parsedPort, err = strconv.ParseInt(string(port), 0, 0)
		if err != nil {
			return Node{}, err
		}
		if parsedPort < 0 {
			return Node{}, fmt.Errorf("Negative port number is illegal")
		}
	}

	return Node{string(hostname), int(parsedPort), string(instance)}, nil
}

func NodeCmp(a, b Node) bool {
	if a.Server != b.Server {
		return false
	}
	if a.Port != b.Port {
		return false
	}
	if a.Instance != b.Instance {
		return false
	}

	return true
}

// computeCarbonRingPosition takes a string and computes where that
// string lives in the 16bit wide hash ring.
func computeCarbonRingPosition(key string) (result int) {
	// digest is our full 64bit hash as a slice of 8 bytes
	digest := md5.Sum([]byte(key))

	// Make an int out of the last 2 bytes for a 16bit ring
	for _, v := range digest[:2] {
		result = (result << 8) + int(v)
	}
	return
}

// bisectLeft returns the insertion index where e should be inserted into ring
// if duplicate e's are already in the list the insertion point will be to the
// left or before the equal entries.
func bisectLeft(ring []RingEntry, e RingEntry) (i int) {
	for i = 0; i < len(ring); i++ {
		if ring[i].position >= e.position {
			break
		}
	}

	return i
}

// cmp compares two RingEntry variables similar to the way that the Python
// code in hashing.py compares nodes in the hashring.
func cmp(a, b RingEntry) int {
	if a.position < b.position {
		return -1
	}
	if a.position > b.position {
		return 1
	}

	// Ok, a.position == b.position
	if a.node.Server < b.node.Server {
		return -1
	}
	if a.node.Server > b.node.Server {
		return 1
	}

	// Now, a.position/server == b.positon/server
	if a.node.Instance < b.node.Instance {
		return -1
	}
	if a.node.Instance > b.node.Instance {
		return 1
	}

	// Out of crazy mess to compare -- must be equal
	return 0
}

// bisectRight returns the insertion index where e should be inserted into ring
// if duplicate e's are already in the list the insertion point will be to the
// right or after the equal entries.
// This is only used for ring insertion and the Python version compares tuples
// so we use a custom cmp function to mimic what the Python code does.
func bisectRight(ring []RingEntry, e RingEntry) (i int) {
	for i = 0; i < len(ring); i++ {
		if cmp(ring[i], e) > 0 {
			break
		}
	}

	return i
}

// insertRing inserts a RingEntry e into the slice ring in the correct
// order.  An updated []RingEntry slice is returned
func insertRing(ring []RingEntry, e RingEntry) []RingEntry {
	// Find where e goes in the ring
	i := bisectRight(ring, e)

	// Extend the underlying array if needed
	ring = append(ring, e)

	if i == len(ring)-1 {
		// last element in slice, or slice was empty, we are done
		return ring
	} else {
		// Move data around to make room for e
		copy(ring[i+1:], ring[i:len(ring)-1])
		ring[i] = e
	}
	return ring
}

// Node.CarbonKeyValue generates the string representation used in the hash
// ring just as Graphite's Python code does.  Useful only for carbon
// style hashrings.
func (t Node) CarbonKeyValue() string {
	if t.Instance == "" {
		return fmt.Sprintf("('%s', None)", t.Server)
	}
	return fmt.Sprintf("('%s', '%s')", t.Server, t.Instance)
}

// Node.String returns a string representation of the Node struct
func (t Node) String() string {
	if t.Instance == "" {
		return fmt.Sprintf("%s:%d=None", t.Server, t.Port)
	}
	return fmt.Sprintf("%s:%d=%s", t.Server, t.Port, t.Instance)
}

func (t *CarbonHashRing) String() string {
	servers := make([]string, 0)

	for i := 0; i < len(t.nodes); i++ {
		servers = append(servers, t.nodes[i].String())
	}
	return fmt.Sprintf("[carbon: %d nodes, %d replicas, %d ring members %s]",
		len(t.nodes), t.replicas, len(t.ring), strings.Join(servers, " "))
}

func (t *CarbonHashRing) Replicas() int {
	return t.replicas
}

func (t *CarbonHashRing) SetReplicas(r int) {
	t.replicas = r
}

func (t *CarbonHashRing) AddNode(node Node) {
	// log.Printf("insertRing(): %s", node.CarbonKeyValue())
	t.nodes = append(t.nodes, node)
	for i := 0; i < t.replicas; i++ {
		var e RingEntry
		replica_key := fmt.Sprintf("%s:%d", node.CarbonKeyValue(), i)
		e.position = computeCarbonRingPosition(replica_key)
		e.node = node
		t.ring = insertRing(t.ring, e)
	}
}

func (t *CarbonHashRing) RemoveNode(node Node) {
	var i int

	// Find node in nodes
	for i = 0; i < len(t.nodes); {
		if node.String() == t.nodes[i].String() {
			t.nodes = append(t.nodes[:i], t.nodes[i+1:]...)
		} else {
			i++
		}
	}

	// Remove matching ring locations
	for i = 0; i < len(t.ring); {
		if node.String() == t.ring[i].node.String() {
			t.ring = append(t.ring[:i], t.ring[i+1:]...)
		} else {
			i++
		}
	}
}

func (t *CarbonHashRing) GetNode(key string) Node {
	if len(t.ring) == 0 {
		panic("HashRing is empty")
	}

	e := RingEntry{computeCarbonRingPosition(key), NewNode(key, 0, "")}
	i := mod(bisectLeft(t.ring, e), len(t.ring))
	// log.Printf("len(ring) = %d", len(t.ring))
	// log.Printf("Bisect index for %s is %d", key, i)
	// log.Printf("Ring position for %s is %x", key, e.position)
	return t.ring[i].node
}

func (t *CarbonHashRing) GetNodes(key string) []Node {
	if len(t.ring) == 0 {
		panic("HashRing is empty")
	}

	result := make([]Node, 0)
	seen := make(map[string]bool)
	e := RingEntry{computeCarbonRingPosition(key), NewNode(key, 0, "")}
	index := mod(bisectLeft(t.ring, e), len(t.ring))
	last := index - 1

	for len(seen) < len(t.nodes) && index != last {
		next := t.ring[index]
		if !seen[next.node.String()] {
			seen[next.node.String()] = true
			result = append(result, next.node)
		}
		index = mod((index + 1), len(t.ring))
	}

	return result
}

func (t *CarbonHashRing) BucketsPerNode() map[string]int {
	if len(t.ring) == 0 {
		panic("HashRing is empty")
	}

	hash := make(map[string]int)
	max := 0xFFFF
	last := t.ring[len(t.ring)-1]
	for i, e := range t.ring {
		buckets := 0
		if i == 0 {
			buckets = (max - last.position) + e.position
		} else {
			buckets = e.position - last.position
		}

		hash[e.node.String()] = hash[e.node.String()] + buckets
		last = e
	}

	return hash
}

// Len return the number of buckets or nodes in the hash ring.
func (t *CarbonHashRing) Len() int {
	return len(t.nodes)
}

// Nodes returns the nodes in the carbon hash ring
func (t *CarbonHashRing) Nodes() []Node {
	return t.nodes
}

// mod returns a modulo b which is not the same as Go's a % b operator.
func mod(a, b int) int {
	return a - (b * (a / b))
}
