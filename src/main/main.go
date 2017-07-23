package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"path"
	"time"

	"encoding/json"

	"github.com/samuel/go-zookeeper/zk"
)

var kZkData *ZkData = nil

func main() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err) // 这里的err其实就是panic传入的内容，55
		}
	}()
	fmt.Println("Welcome to zk-agent.")
	configPath := flag.String("config", "", "Location of configuration file")
	// FOR Debug begin
	debugPath := "D:/Users/StAR/Documents/DevProjects/zk-agent/config.json"
	configPath = &debugPath
	// FOR Debug end

	config, err := parseConfig(*configPath)
	if err != nil {
		panic(err)
	}
	ec, err := zkAgentStart(config)
	if err != nil {
		panic(err)
	}
	for {
		event, isAlive := <-ec
		fmt.Println(event)
		if !isAlive {
			fmt.Println("zk-agent shutdown.")
			return
		}
	}
}

func parseConfig(configPath string) (map[string]interface{}, error) {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, err
	}
	var jsonObj map[string]interface{}
	if err := json.Unmarshal([]byte(data), &jsonObj); err != nil {
		return nil, err
	}
	return jsonObj, nil
}

func zkAgentStart(config map[string]interface{}) (<-chan zk.Event, error) {
	zkServerOpt := config["zkServer"]
	var zkServers []string
	switch zkServerOpt.(type) {
	case string:
		val := zkServerOpt.(string)
		zkServers = []string{val}
	case []string:
		zkServers = zkServerOpt.([]string)
	}
	// setup connection
	conn, event, err := zk.Connect(zkServers, 10*time.Second)
	if err != nil {
		return nil, err
	}
	// defer conn.Close()

	// get and watch data
	zkDataPathOpt := config["zkDataPath"]
	var zkDataPaths []string
	switch zkDataPathOpt.(type) {
	case string:
		val := zkDataPathOpt.(string)
		zkDataPaths = []string{val}
	case []string:
		zkDataPaths = zkDataPathOpt.([]string)
	}
	zkData, _ /*events*/, err := watchZkPaths(zkDataPaths, conn)
	if err != nil {
		return nil, err
	}
	kZkData = zkData
	fmt.Println(kZkData)
	return event, nil
}

type ZkData struct {
	Data map[string]ZkNode
	Conn *zk.Conn
}

type ZkNode struct {
	Path   string
	Stat   zk.Stat
	Childs []string
	Value  string
}

func (self *ZkData) String() string {
	bData, _ := json.Marshal(self.Data)
	if bData != nil {
		return string(bData)
	}
	return ""
}

func (self *ZkData) fillDatas(paths []string) error {
	conn := self.Conn
	for _, _path := range paths {
		childs, stat, err := conn.Children(_path)
		if err != nil {
			return err
		}
		bData, _, err := conn.Get(_path)
		if err != nil {
			return err
		}
		val := string(bData)
		node := ZkNode{
			Path:   _path,
			Stat:   *stat,
			Childs: childs,
			Value:  val,
		}
		self.Data[_path] = node
		if len(childs) > 0 {
			subPaths := make([]string, 0, 10)
			for _, v := range childs {
				subPath := path.Join(_path, v)
				subPaths = append(subPaths, subPath)
			}
			self.fillDatas(subPaths)
		}
	}
	return nil
}

func watchZkPaths(paths []string, conn *zk.Conn) (zkData *ZkData, events []<-chan zk.Event, err error) {
	zkData = &ZkData{
		Conn: conn,
		Data: make(map[string]ZkNode),
	}
	events = make([]<-chan zk.Event, 0, 32)
	for _, _path := range paths {
		childs, stat, event, err := conn.ChildrenW(_path)
		if err != nil {
			return nil, nil, err
		}
		bData, _, err := conn.Get(_path)
		if err != nil {
			return nil, nil, err
		}
		val := string(bData)
		node := ZkNode{
			Path:   _path,
			Stat:   *stat,
			Childs: childs,
			Value:  val,
		}
		zkData.Data[_path] = node
		if len(childs) > 0 {
			subPaths := make([]string, 0, 10)
			for _, v := range childs {
				subPath := path.Join(_path, v)
				subPaths = append(subPaths, subPath)
			}
			zkData.fillDatas(subPaths)
		}
		events = append(events, event)
	}
	return zkData, events, nil
}
