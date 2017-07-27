package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"text/template"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

var kZkData *ZkData = nil

func main() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()
	fmt.Println("Welcome to zk-agent.")
	configPath := flag.String("config", "", "Location of configuration file")
	// FOR Debug begin
	debugPath := "/Users/tan/Documents/GitHub/zk-agent/config.json"
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
	commandOpt, ok := config["shellCommand"]
	var command string
	if ok {
		command, ok = commandOpt.(string)
		if !ok {
			return nil, errors.New("Invalid `shellCommand` format.")
		}
	}
	zkServerOpt := config["zkServer"]
	var zkServers []string
	switch zkServerOpt.(type) {
	case string:
		val := zkServerOpt.(string)
		zkServers = []string{val}
	case []string:
		zkServers = zkServerOpt.([]string)
	case []interface{}:
		for _, v := range zkServerOpt.([]interface{}) {
			zkServers = append(zkServers, v.(string))
		}
	}
	// setup connection
	conn, eventChan, err := zk.Connect(zkServers, 10*time.Second)
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
	case []interface{}:
		for _, v := range zkDataPathOpt.([]interface{}) {
			zkDataPaths = append(zkDataPaths, v.(string))
		}
	}
	zkData, err := CreateZkData(zkDataPaths, conn)
	if err != nil {
		return nil, err
	}
	// TODO Event listener
	kZkData = zkData

	// Generate target file
	combineOpt := config["combine"]
	var combines []string
	switch combineOpt.(type) {
	case string:
		val := combineOpt.(string)
		combines = []string{val}
	case []string:
		combines = combineOpt.([]string)
	case []interface{}:
		for _, v := range combineOpt.([]interface{}) {
			combines = append(combines, v.(string))
		}
	}

	var tmpl, target string
	for _, v := range combines {
		tmplAndTarget := strings.Split(v, "#")
		if len(tmplAndTarget) != 2 {
			return nil, errors.New("Invalid `combine` format.")
		}
		tmpl = tmplAndTarget[0]
		target = tmplAndTarget[1]
		err := rebuildDataFile(kZkData, tmpl, target)
		if err != nil {
			return nil, err
		}
	}

	// Keep Listening
	keventChan := make(chan zk.Event, 10)
	go func() {
		for {
			event, ok := <-eventChan
			if !ok {
				close(keventChan)
				return
			}
			var err error
			switch event.Type {
			case zk.EventNodeDataChanged:
				fmt.Println("NodeDataChanged: " + event.Path)
				err = reload(event.Path, tmpl, target, command)
			case zk.EventNodeChildrenChanged:
				fmt.Println("NodeChildrenChanged: " + event.Path)
				err = reload(event.Path, tmpl, target, command)
			default:
				fmt.Println(event)
			}
			if err != nil {
				// Warn
				fmt.Println(err)
			}
			keventChan <- event
		}
	}()

	return keventChan, nil
}

func reload(nodePath string, tmplPath string, targetPath string, commandTmpl string) error {
	kZkData.GetNodesW([]string{nodePath})
	// TODO: Rebuild and invoke command when zkSwitcherPath change
	err := rebuildDataFile(kZkData, tmplPath, targetPath)
	if err != nil {
		return fmt.Errorf("Rebuild data file failed, cause by: %+v", err)
	}
	// build command
	if len(commandTmpl) == 0 {
		return nil
	}
	tmpl, err := template.New("command").Funcs(template.FuncMap{"dat": getByKey}).Parse(commandTmpl)
	if err != nil {
		return err
	}
	buffer := bytes.NewBuffer([]byte{})
	tmpl.Execute(buffer, kZkData.Data)
	command := buffer.String()

	// invoke command
	var cmd *exec.Cmd
	switch os := runtime.GOOS; os {
	case "windows":
		cmd = exec.Command("cmd", "/c", command)
	default:
		cmd = exec.Command("sh", "-c", command)
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("Execute command `%+v` failed, cause by: %+v", command, err)
	}
	fmt.Println(string(out))
	return nil
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

func (self *ZkData) deleteOldData(nodePath string) {
	node, ok := self.Data[nodePath]
	if !ok {
		return
	}
	if len(node.Childs) == 0 {
		delete(self.Data, nodePath)
		return
	}
	for _, childName := range node.Childs {
		childPath := path.Join(nodePath, childName)
		self.deleteOldData(childPath)
	}
}

func (self *ZkData) GetNodesW(paths []string) (err error) {
	conn := self.Conn
	for _, _path := range paths {
		// Clean old data first
		self.deleteOldData(_path)

		childs, stat, _, err := conn.ChildrenW(_path)
		if err != nil {
			return err
		}
		bData, _, _, err := conn.GetW(_path)
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
			err := self.GetNodesW(subPaths)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func CreateZkData(paths []string, conn *zk.Conn) (zkData *ZkData, err error) {
	zkData = &ZkData{
		Conn: conn,
		Data: make(map[string]ZkNode),
	}
	err = zkData.GetNodesW(paths)
	if err != nil {
		err = fmt.Errorf("Watch nodes failed, cause by: %+v", err)
	}
	return zkData, err
}

func rebuildDataFile(zkData *ZkData, tmplPath string, targetPath string) error {
	data := zkData.Data
	tdata, err := ioutil.ReadFile(tmplPath)
	if err != nil {
		return err
	}
	tmplData := string(tdata)
	basename := path.Base(targetPath)
	tmpl, err := template.New(basename).Funcs(template.FuncMap{"dat": getByKey}).Parse(tmplData)
	if err != nil {
		return err
	}
	buffer := bytes.NewBuffer([]byte{})
	tmpl.Execute(buffer, data)
	targetData := []byte(buffer.String())
	err = ioutil.WriteFile(targetPath, targetData, os.ModePerm)
	if err != nil {
		return err
	}
	return nil
}

func getByKey(data interface{}, keys ...string) (res interface{}) {
	defer func() {
		err := recover()
		if err != nil {
			fmt.Println(err)
			res = nil
		}
	}()
	for _, k := range keys {
	start:
		rdata := reflect.ValueOf(data)
		switch rdata.Kind() {
		case reflect.Map:
			data = rdata.MapIndex(reflect.ValueOf(k)).Interface()
		case reflect.Ptr:
			data = rdata.Elem()
			goto start
		case reflect.Array:
			continue
		case reflect.Slice:
			idx, err := strconv.Atoi(k)
			if err != nil {
				panic(err)
			}
			data = rdata.Index(idx).Interface()
		default:
			data = rdata.FieldByName(k).Interface()
		}
	}
	return data
}
