package consistentHash

import (
	"encoding/binary"
	"errors"
	"sort"
	"sync"
)

const (
	defaultPreAllocNodeCount = 8
	defaultVirtualNodeCount  = 10
)

var (
	errNodeExist = errors.New("node exist")
)

type (
	// Node 普通节点
	Node interface {
		Key() []byte
	}
	// RehashNode 加入或离开集群时需要转移数据的节点
	RehashNode interface {
		Node
		Migrate(s *ConsistentHash, sepVNodeSlotID uint32, dstNode RehashNode)
		Transfer(s *ConsistentHash)
	}
)

type ConsistentHash struct {
	h                HashFunc          // hash 函数
	virtualNodeCount int               // 单个节点的虚拟节点数量
	vnodes           []uint32          // 虚拟节点插槽id列表
	vNodeMapActNodes map[uint32][]Node // 虚拟节点映射真实节点列表
	nodeIDMapNode    map[uint32]Node   // 真实节点id映射真实节点
	mu               sync.RWMutex      // 读写锁
}

func New() *ConsistentHash {
	return &ConsistentHash{
		h:                Fnv1a,
		mu:               sync.RWMutex{},
		vnodes:           make([]uint32, 0, defaultPreAllocNodeCount*defaultVirtualNodeCount),
		vNodeMapActNodes: make(map[uint32][]Node, defaultPreAllocNodeCount*defaultVirtualNodeCount),
		virtualNodeCount: defaultVirtualNodeCount,
		nodeIDMapNode:    make(map[uint32]Node, defaultPreAllocNodeCount),
	}
}

// AddNode 向集群中添加节点
func (s *ConsistentHash) AddNode(node Node) error {
	nodeID := s.h(node.Key())
	if _, exist := s.nodeIDMapNode[nodeID]; exist {
		return errNodeExist
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodeIDMapNode[nodeID] = node // 添加真实节点
	vNodeSlotIDArr := make([]uint32, 0, s.virtualNodeCount)
	currVNodeSlotIDSet := make(map[uint32]struct{}, s.virtualNodeCount)
	for i := 0; i < s.virtualNodeCount; i++ { // 添加虚拟节点
		vNodeSlotID := s.u32Hash(nodeID + uint32(i))
		vNodeSlotIDArr = append(vNodeSlotIDArr, vNodeSlotID)
		currVNodeSlotIDSet[vNodeSlotID] = struct{}{}
		if nodes, exist := s.vNodeMapActNodes[vNodeSlotID]; exist {
			nodes = append(nodes, node)
			s.vNodeMapActNodes[vNodeSlotID] = nodes
		} else {
			s.vnodes = append(s.vnodes, vNodeSlotID)
			s.vNodeMapActNodes[vNodeSlotID] = []Node{node}
		}
	}
	sort.Slice(s.vnodes, func(i, j int) bool { return s.vnodes[i] < s.vnodes[j] }) // 排序所有虚拟节点
	// rehash
	_, ok := node.(RehashNode)
	if !ok {
		return nil
	}
	if len(s.nodeIDMapNode) == 1 {
		return nil
	}
	sort.Slice(vNodeSlotIDArr, func(i, j int) bool { return vNodeSlotIDArr[i] < vNodeSlotIDArr[j] }) // 排序新虚拟节点
	for i := 0; i < s.virtualNodeCount; i++ {
		vNodeSlotID := vNodeSlotIDArr[i]
		vNodeCount := uint32(len(s.vnodes))
		nextVNodeSlotID := uint32(sort.Search(int(vNodeCount), func(i int) bool {
			_, exist := currVNodeSlotIDSet[s.vnodes[i]]
			return s.vnodes[i] > vNodeSlotID && !exist
		}))
		if nextVNodeSlotID == vNodeCount {
			nextVNodeSlotID = s.vnodes[0] // 未找到时从第一个迁移
		}
		for _, nextNode := range s.vNodeMapActNodes[nextVNodeSlotID] {
			nextNode.(RehashNode).Migrate(s, vNodeSlotID, node.(RehashNode)) // 将下一个节点平衡至此节点
		}
	}
	return nil
}

// RemoveNode 从集群中删除节点
func (s *ConsistentHash) RemoveNode(nodeKey []byte) {
	nodeID := s.h(nodeKey)
	node, exist := s.nodeIDMapNode[nodeID]
	if !exist {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.nodeIDMapNode, nodeID)
	for i := 0; i < s.virtualNodeCount; i++ { // 移除虚拟节点
		vNodeSlotID := s.u32Hash(nodeID + uint32(i))
		nodes := s.vNodeMapActNodes[vNodeSlotID]
		if len(nodes) > 1 { // 从映射中移除当前节点
			newNodes := nodes[:0]
			for _, v := range nodes {
				if v != node {
					newNodes = append(newNodes, v)
				}
			}
			if len(newNodes) > 0 {
				s.vNodeMapActNodes[vNodeSlotID] = newNodes
			}
		} else { // 删除当前节点映射和虚拟节点
			delete(s.vNodeMapActNodes, vNodeSlotID)
			vNodeIdx := sort.Search(len(s.vnodes), func(j int) bool {
				return s.vnodes[j] >= vNodeSlotID
			})
			if vNodeIdx < len(s.vnodes) { // 有一种可能某个节点的虚拟节点重复了 就不需要重复删除了
				s.vnodes = append(s.vnodes[:vNodeIdx], s.vnodes[vNodeIdx+1:]...)
			}
		}
	}
	sort.Slice(s.vnodes, func(i, j int) bool { return s.vnodes[i] < s.vnodes[j] }) // 排序所有虚拟节点
	// rehash
	rehashNode, ok := node.(RehashNode)
	if !ok {
		return
	}
	if len(s.nodeIDMapNode) == 0 {
		return
	}
	rehashNode.Transfer(s)
}

// GetNode 找到key对应节点
func (s *ConsistentHash) GetNode(nodeKey []byte) Node {
	hashIdx := s.h(nodeKey)
	vNodeCount := len(s.vnodes)
	vNodeIdx := sort.Search(vNodeCount, func(i int) bool {
		return s.vnodes[i] >= hashIdx
	})
	if vNodeIdx == vNodeCount {
		vNodeIdx = 0 // 未找到时放到第一个虚拟节点
	}
	nodes, exist := s.vNodeMapActNodes[s.vnodes[vNodeIdx]]
	if !exist {
		return nil
	}
	if len(nodes) == 1 {
		return nodes[0]
	}
	return nodes[hashIdx%uint32(len(nodes))]
}

func (s *ConsistentHash) Hash(data []byte) uint32 {
	return s.h(data)
}

func (s *ConsistentHash) u32Hash(val uint32) uint32 {
	return s.h(binary.LittleEndian.AppendUint32(make([]byte, 0, 4), val))
}
