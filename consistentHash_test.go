package consistentHash

import (
	"encoding/binary"
	"math"
	"strconv"
	"sync"
	"testing"
	"unsafe"
)

var (
	_ Node       = (*Cache)(nil)
	_ RehashNode = (*Cache)(nil)
)

// 缓存节点 测试用
type Cache struct {
	id string
	m  sync.Map     // 实际存储区
	mu sync.RWMutex // map 读写锁
}

func (ce *Cache) Key() []byte {
	return str2Bytes(ce.id)
}

func NewCache(id string) *Cache {
	return &Cache{
		id: id,
		m:  sync.Map{},
		mu: sync.RWMutex{},
	}
}
func (ce *Cache) get(key int) (any, bool) {
	return ce.m.Load(key)
}
func (ce *Cache) set(key int, value any) {
	ce.m.Store(key, value)
}
func (ce *Cache) del(key int) {
	ce.m.Delete(key)
}

// Migrate 迁移当前节点中分割插槽之前的数据到目标节点
func (ce *Cache) Migrate(s *ConsistentHash, sepVCacheSlotID uint32, dstCache RehashNode) {
	dstCe := dstCache.(*Cache)
	if ce == dstCe {
		return
	}
	ce.m.Range(func(key, value any) bool {
		slotID := s.Hash(int2Bytes(key.(int)))
		if slotID <= sepVCacheSlotID {
			dstCe.set(key.(int), value)
			ce.del(key.(int))
		}
		return true
	})
}

// Transfer 转移当前节点中所有数据
func (ce *Cache) Transfer(s *ConsistentHash) {
	ce.m.Range(func(key, value any) bool {
		node := s.GetNode(binary.LittleEndian.AppendUint64(make([]byte, 0, 8), uint64(key.(int))))
		node.(*Cache).set(key.(int), value)
		ce.del(key.(int))
		return true
	})
}
func str2Bytes(s string) []byte {
	return *(*[]byte)(unsafe.Pointer(&s))
}

func int2Bytes(i int) []byte {
	return binary.LittleEndian.AppendUint64(make([]byte, 0, 8), uint64(i))
}

func TestNewServer(t *testing.T) {
	t.Run("测试一致性哈希", func(t *testing.T) {
		s := New()
		n := 200
		dataCount := 10
		for i := 0; i < n; i++ {
			_ = s.AddNode(NewCache(strconv.Itoa(i)))
		}
		nodeMapValCount := make(map[string]int, n)
		for i := 0; i < n*dataCount; i++ {
			node := s.GetNode(int2Bytes(i))
			node.(*Cache).set(i, i)
			nodeMapValCount[string(node.Key())]++
		}
		deviationVal := 0.0
		for _, i := range nodeMapValCount {
			deviationVal += math.Pow(float64(i-dataCount), float64(2))
		}
		t.Logf("数据平衡方差:%.2f\n", deviationVal/float64(n))
		for i := 0; i < n/2; i++ {
			s.RemoveNode(int2Bytes(i))
		}
		for i := 0; i < n*dataCount; i++ {
			if v, exist := s.GetNode(int2Bytes(i)).(*Cache).get(i); !exist || v.(int) != i {
				t.Errorf("key not found:%d\n", i)
			}
		}
	})
}
