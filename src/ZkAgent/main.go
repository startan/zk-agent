package main

import (
	za "ZkAgent/server"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
)

func main() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()
	fmt.Println("Welcome to zk-agent.")
	configPath := flag.String("config", "", "Location of configuration file")
	// FOR Debug begin
	debugPath := "config.json"
	configPath = &debugPath
	// FOR Debug end

	config, err := parseConfig(*configPath)
	if err != nil {
		panic(err)
	}
	ec, err := za.ZkAgentStart(config)
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
