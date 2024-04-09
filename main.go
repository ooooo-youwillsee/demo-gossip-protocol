package main

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"time"
)

type ID int64

type Node struct {
	Id   ID
	Addr string
	Port int
	// 上次联系时间
	lastContact time.Time
	// 所有的节点列表
	members map[ID]*Node
	// 每个节点都会携带一部分数据
	data map[string]string
}

// NewNode 创建新节点
func NewNode(id ID, addr string, port int) *Node {
	return &Node{
		Id:          id,
		Addr:        addr,
		Port:        port,
		lastContact: time.Now(),
		members:     make(map[ID]*Node, 0),
		data:        make(map[string]string),
	}
}

func (n *Node) AddMember(node *Node) error {
	if _, ok := n.members[node.Id]; ok {
		return fmt.Errorf("node %s is already member of this node", node.Id)
	}
	n.members[node.Id] = node
	return nil
}

// Start 启动
func (n *Node) Start() string {
	t := time.NewTicker(1 * time.Second)
	for {
		<-t.C
		n.sync()
	}
}

func (n *Node) sync() {
	// 随机选择一个节点
	targetNode := n.selectRandomNode()
	if targetNode == nil {
		log.Printf("%s, not sync, targetNode is nil", n)
		return
	}

	// 发送请求
	err := n.call(targetNode, n.members)
	if err != nil {
		// 节点超时, 应该加入失败节点列表，然后广播所有节点判断是否应该剔除
		log.Printf("self: %s call fail, targetNode is %v", n, targetNode)
		return
	}
	log.Printf("self: %s sync success, targetNode is %v", n, targetNode)
	targetNode.lastContact = time.Now()
}

// 模拟被调用方的逻辑
func (n *Node) call(targetNode *Node, members map[ID]*Node) error {
	// 更新成员
	for id, m := range members {
		if _, ok := targetNode.members[id]; !ok {
			targetNode.members[id] = m
		}
	}
	// 更新时间
	targetNode.members[n.Id] = n
	targetNode.members[n.Id].lastContact = time.Now()
	return nil
}

func (n *Node) selectRandomNode() *Node {
	if len(n.members) == 0 {
		return nil
	}
	i := rand.Int() % len(n.members)
	for _, m := range n.members {
		if i == 0 {
			return m
		}
		i -= 1
	}
	return nil
}

func (n *Node) String() string {
	return "node:" + n.Addr + ":" + strconv.Itoa(int(n.Id))
}

func main() {
	allNode := make(map[ID]*Node, 0)
	n1 := NewNode(ID(1), "127.0.0.1", 8081)
	allNode[n1.Id] = n1
	n2 := NewNode(ID(2), "127.0.0.1", 8082)
	allNode[n2.Id] = n2
	n3 := NewNode(ID(3), "127.0.0.1", 8083)
	allNode[n3.Id] = n3
	n4 := NewNode(ID(4), "127.0.0.1", 8084)
	allNode[n4.Id] = n4
	n5 := NewNode(ID(5), "127.0.0.1", 8085)
	allNode[n5.Id] = n5

	n1.AddMember(n2)
	n1.AddMember(n3)

	n3.AddMember(n4)
	n3.AddMember(n5)

	for _, n := range allNode {
		go n.Start()
	}

	// 检查所有节点是否同步完成
	check(allNode)
}

func check(allNode map[ID]*Node) {
	t := time.NewTicker(5 * time.Second)
	for {
		<-t.C
		ok := true
		for _, n := range allNode {
			if len(n.members) != len(allNode) {
				ok = false
				break
			}
		}
		if ok {
			log.Printf("synced all nodes: %v", allNode)
			return
		}
	}
}
