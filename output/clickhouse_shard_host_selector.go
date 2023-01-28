package output

import "sort"

// Shard Key
type ShardKeyHostSelector struct {
	hosts           []ShardKeyHost
	shardNodes      map[int][]ShardKeyHost
	totalWeight     int //总权重
	hashShardNumber map[int]int
}

type ShardKeyHost struct {
	Addr        string
	ShardNum    int
	ShardWeight int
}

func NewShardKeyHostSelector(hosts []ShardKeyHost) *ShardKeyHostSelector {
	shardNodes := make(map[int][]ShardKeyHost)
	totalWeight := 0
	shardWeight := make(map[int]int)
	shardNumbers := make([]int, 0)
	for _, node := range hosts {
		if shardNodes[node.ShardNum] == nil {
			shardNodes[node.ShardNum] = make([]ShardKeyHost, 0)
			totalWeight += node.ShardWeight
			shardWeight[node.ShardNum] = node.ShardWeight
			shardNumbers = append(shardNumbers, node.ShardNum)
		}
		shardNodes[node.ShardNum] = append(shardNodes[node.ShardNum], node)
	}

	sort.Ints(shardNumbers)
	hashShardNumber := make(map[int]int, 0)
	prevWeightSum := 0
	for _, sno := range shardNumbers {
		for i := 0; i < shardWeight[sno]; i++ {
			hashShardNumber[prevWeightSum] = sno
			prevWeightSum += 1
		}
	}

	return &ShardKeyHostSelector{
		hosts:           hosts,
		shardNodes:      shardNodes,
		totalWeight:     totalWeight,
		hashShardNumber: hashShardNumber,
	}
}
