package memdb

import (
	"encoding/json"
	"testing"
)

func TestSetSerialization(t *testing.T) {
	// 测试用例1：正常集合
	t.Run("NormalSet", func(t *testing.T) {
		set := NewSet()
		set.Add("a")
		set.Add("b")
		set.Add("c")

		// 序列化
		data, err := json.Marshal(set)
		if err != nil {
			t.Fatalf("序列化失败: %v", err)
		}

		// 反序列化
		var newSet Set
		if err := json.Unmarshal(data, &newSet); err != nil {
			t.Fatalf("反序列化失败: %v", err)
		}

		// 验证元素数量和内容
		if newSet.Size() != 3 {
			t.Errorf("期望元素数量=3，实际数量=%d", newSet.Size())
		}
		for _, key := range []string{"a", "b", "c"} {
			if !newSet.Contains(key) {
				t.Errorf("元素 %s 未找到", key)
			}
		}
	})

	// 测试用例2：空集合
	t.Run("EmptySet", func(t *testing.T) {
		set := NewSet()

		// 序列化
		data, err := json.Marshal(set)
		if err != nil {
			t.Fatalf("序列化失败: %v", err)
		}

		// 反序列化
		var newSet Set
		if err := json.Unmarshal(data, &newSet); err != nil {
			t.Fatalf("反序列化失败: %v", err)
		}

		// 验证空集合
		if newSet.Size() != 0 {
			t.Errorf("期望空集合，实际元素数量=%d", newSet.Size())
		}
	})

	// 测试用例3：特殊字符和转义
	t.Run("SpecialCharacters", func(t *testing.T) {
		set := NewSet()
		set.Add(`te"st`)
		set.Add("line\nbreak")
		set.Add("back\\slash")

		// 序列化
		data, err := json.Marshal(set)
		if err != nil {
			t.Fatalf("序列化失败: %v", err)
		}

		// 反序列化
		var newSet Set
		if err := json.Unmarshal(data, &newSet); err != nil {
			t.Fatalf("反序列化失败: %v", err)
		}

		// 验证特殊字符
		expected := []string{`te"st`, "line\nbreak", "back\\slash"}
		for _, s := range expected {
			if !newSet.Contains(s) {
				t.Errorf("元素 %s 未找到", s)
			}
		}
	})

	// 测试用例4：非法JSON数据
	t.Run("InvalidJSON", func(t *testing.T) {
		// 测试非数组格式
		invalidData := []byte(`{"key": "value"}`)
		var set1 Set
		if err := json.Unmarshal(invalidData, &set1); err == nil {
			t.Error("预期反序列化失败，但未报错")
		}

		// 测试数组包含非字符串元素
		invalidArray := []byte(`[1, true, null]`)
		var set2 Set
		if err := json.Unmarshal(invalidArray, &set2); err == nil {
			t.Error("预期反序列化失败，但未报错")
		}
	})
}

// 辅助函数：创建新集合
// 辅助函数：检查元素是否存在
func (s *Set) Contains(key string) bool {
	_, ok := s.table[key]
	return ok
}

// 辅助函数：获取元素数量
func (s *Set) Size() int {
	return len(s.table)
}
