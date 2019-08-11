package main

import "encoding/json"
import "fmt"
import "io/ioutil"
import "os"
import "os/exec"
import "time"
import "flag"
import "strconv"
import "log"
import "strings"
import "errors"
import "math/rand"

type PartitionMap struct {
	Version    int         `json:"version"`
	Partitions []Partition `json:"partitions"`
}

type Partition struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Replicas  []int  `json:"replicas"`
}

type TopicPartion struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
}

type PreferedLeaderPartition struct {
	Partitions []TopicPartion `json:"partitions"`
}

var (
	zks         = "staging-kafka-1.svr.tiki.services:2181"
	brokerCount = 0
	genRandom   = rand.New(rand.NewSource(time.Now().UnixNano()))
	throttle    = 20971520
)

//brokers := "staging-kafka-1.svr.tiki.services:9092"

//kafka-topics.sh --zookeeper staging-kafka-1.svr.tiki.services:2181 --list
func GetTopics(topicReg string) []string {
	//command := "kafka-topics.sh " + brokers + " --list --topic" + topicReg
	out, err := exec.Command("bash", "kafka-topics.sh", "--zookeeper", zks, "--list", "--topic", topicReg).Output()

	if err != nil {
		log.Fatal(err)
		return nil
	}

	data := strings.Split(string(out), "\n")
	var topics []string
	for _, line := range data {
		if len(line) != 0 && line != " " {
			topics = append(topics, line)
		}
	}
	return topics
}

//get current partition map
//./kafka-reassign-partitions.sh --zookeeper staging-kafka-1.svr.tiki.services:2181 --topics-to-move-json-file test/poc-reassign-topic-request.json --generate --broker-list 1,2,3
func GetPartitionsMap(topic string) PartitionMap {

	if topic == "" {
		fmt.Println("Topic is empty")
		return PartitionMap{}
	}

	topicjson := []byte("{\"topics\":[{\"topic\": \"" + topic + "\"}],\"version\":1}")
	err := ioutil.WriteFile("/tmp/test-topic.json", topicjson, 0644)

	out, err := exec.Command("bash", "kafka-reassign-partitions.sh", "--zookeeper", zks, "--topics-to-move-json-file", "/tmp/test-topic.json",
		"--generate", "--broker-list", "1,2,3").Output()

	if err != nil {
		log.Fatal(err)
		return PartitionMap{}
	}

	data := strings.Split(string(out), "\n")

	var partMap PartitionMap
	err = json.Unmarshal([]byte(data[1]), &partMap)

	if err != nil {
		log.Fatal(err)
		fmt.Println("Convert Json Partition Map error")
		return PartitionMap{}
	}

	err = ioutil.WriteFile(topic+"-current-map.json", []byte(data[1]), 0644)

	if err != nil {
		log.Fatal(err)
		fmt.Println("Write file current-map Error")
		return PartitionMap{}
	}

	return partMap

}

func RunAssignPartition(topic string, newPartitionMap PartitionMap) error {

	if len(newPartitionMap.Partitions) == 0 {
		return errors.New("Partition map is null")
	}

	f, err := os.OpenFile(topic+"-exec-map.json", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal(err)
		return nil
	}

	//save the map to file
	execMapData, err := json.Marshal(newPartitionMap)
	_, err = f.Write(execMapData)

	_, err = exec.Command("bash", "kafka-reassign-partitions.sh", "--zookeeper", zks,
		"--reassignment-json-file", topic+"-exec-map.json", "--throttle", strconv.Itoa(throttle), "--execute").Output()
	//TODO: remove return in here
	fmt.Printf("Map after change: %v\n", newPartitionMap)

	if err != nil {
		log.Fatal(err)
		fmt.Println("Execute Reassign Has Errors")
		return err
	}

	fSuccess := false
	for fSuccess == false {
		out, _ := exec.Command("bash", "kafka-reassign-partitions.sh", "--zookeeper", zks,
			"--reassignment-json-file", topic+"-exec-map.json", "--verify").Output()

		if strings.Count(string(out), "successfully") == len(newPartitionMap.Partitions) {
			fSuccess = true // Break if no partition replicate is processing
			break
		}

		if strings.Contains(string(out), "failed") == true {
			return errors.New("ERROR: fatal- failed to reassign partition")
		}
		fmt.Println(string(out))
		fmt.Println("Not yet finish, Sleep 1p")
		time.Sleep(time.Second * 60) //sleep 20s
	}

	fmt.Printf("-----Success: TOPIC %v reassign successfully---------\n", topic)
	return nil
}

func AddNewSlaveReplica(brokerIds []int, currentPartitionMap PartitionMap) (PartitionMap, error) {
	if len(currentPartitionMap.Partitions) == 0 {
		return PartitionMap{}, errors.New("Partition map is null")
	}
	if len(currentPartitionMap.Partitions[0].Replicas) >= 3 {
		return PartitionMap{}, errors.New("Number of replicas of this topic >= 3, please manually handle it !")
	}
	//check which topic we add
	fHasAddNewBroker := false
	for _, p := range currentPartitionMap.Partitions {
		for _, brID := range brokerIds {
			for _, replicas := range p.Replicas {
				if brID == replicas {
					fHasAddNewBroker = true
					break
				}
			}
			if fHasAddNewBroker {
				break
			}
		}
		if fHasAddNewBroker {
			break
		}
	}
	if fHasAddNewBroker {
		fmt.Printf("Error100: topic %v has added new Broker in Replicas \n", currentPartitionMap.Partitions[0].Topic)
		return PartitionMap{}, errors.New("Error100: topic " + currentPartitionMap.Partitions[0].Topic + " has added new Broker in Replicas")
	}
	numberOfBroker := len(brokerIds)
	brokerCount = genRandom.Intn(numberOfBroker)
	for i, p := range currentPartitionMap.Partitions {
		tryValue := brokerIds[brokerCount%numberOfBroker]
		p.Replicas = append(p.Replicas, tryValue)
		currentPartitionMap.Partitions[i] = p
		brokerCount++
	}
	return currentPartitionMap, nil
}

func ChangeLeaderPartition(newbroker map[int]int, part_map PartitionMap, topic string) error {
	//change new leader partition
	for i, p := range part_map.Partitions {
		isFoundNewBroker := false
		for j, pvalue := range p.Replicas {
			if newbroker[pvalue] == 1 {
				temp := p.Replicas[0]
				p.Replicas[0] = pvalue
				p.Replicas[j] = temp
				isFoundNewBroker = true
				break
			}
		}
		if isFoundNewBroker == false {
			//if not found new broker in list, may be it not reassign parts to new broker list, so we must manually handle for sure
			return errors.New("Warnning: None new brokder ids in list of replicas of topic " + topic + " Manually Handle")
		}
		part_map.Partitions[i] = p
	}

	//Apply new map with changed leader partitions
	return RunAssignPartition(topic, part_map)
}

func TriggerChangeLeaderPartitions(topic string, part_map PartitionMap, brokerIDMap map[int]int) error {
	var preferedPart PreferedLeaderPartition

	//test whether our new brokerIDs is first order on replicas list
	i := 0
	for i = 0; i < len(part_map.Partitions[0].Replicas); i++ {
		if brokerIDMap[part_map.Partitions[0].Replicas[i]] == 1 {
			break
		}
	}
	if i >= len(part_map.Partitions[0].Replicas) {
		fmt.Println("Warning: No new brokers in partition replicas, Take a look")
	}
	//generate json file
	for i := range part_map.Partitions {
		tempTopicPart := TopicPartion{Topic: topic, Partition: i}
		preferedPart.Partitions = append(preferedPart.Partitions, tempTopicPart)
	}

	f, err := os.OpenFile("/tmp/topic-prefered-leader-part.json", os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Fatal(err)
		return nil
	}

	//save the map to file
	execMapData, err := json.Marshal(preferedPart)
	_, err = f.Write(execMapData)

	//execute command
	out, err := exec.Command("bash", "kafka-preferred-replica-election.sh", "--zookeeper", zks,
		"--path-to-json-file", "/tmp/topic-prefered-leader-part.json").Output()

	if err != nil {
		log.Fatal(err)
		fmt.Println("ERROR: Execute Preferred Replica Election Has Errors")
		return err
	}
	fmt.Printf("Topic %v: %v\n", topic, string(out))
	return nil
}

func main() {

	flag.StringVar(&zks, "zookeeper", "staging-kafka-1.svr.tiki.services:2181", "Zookeeper url")
	var listBrokers string
	flag.StringVar(&listBrokers, "new-brokers", "", "List new Brokers")
	var ptopic string
	flag.StringVar(&ptopic, "topic", "", "Topic to change (accept regex)")
	var cmd string
	flag.StringVar(&cmd, "cmd", "", "List command, must be: reassign,swapleader,trigger-change-leader")
	flag.IntVar(&throttle, "throttle", 8000000, "Bandwith throttle")

	flag.Parse()

	var brokerIds []int
	brokerIDMap := make(map[int]int)
	for _, id := range strings.Split(listBrokers, ",") {
		v, err := strconv.Atoi(id)
		if err != nil {
			fmt.Println("Parse Broker list Error ", err.Error())
			os.Exit(1)
		}
		brokerIds = append(brokerIds, v)
		brokerIDMap[v] = 1
	}

	topics := GetTopics(ptopic)
	fmt.Println(topics)

	switch cmd {
	case "reassign":
		for _, tp := range topics {
			if tp == "" || tp == " " {
				break
			}
			partMap := GetPartitionsMap(tp)
			fmt.Println("parititon map: ", partMap)
			newPartitionMap, err := AddNewSlaveReplica(brokerIds, partMap)
			if err != nil {
				log.Print(err)
				continue // omit RunAssigPartition task
			}

			err = RunAssignPartition(tp, newPartitionMap)
			if err != nil {
				panic(err)
			}
		}
		break
	case "swapleader":
		for _, tp := range topics {
			if tp == "" || tp == " " {
				break
			}
			partMap := GetPartitionsMap(tp)
			fmt.Println("parititon map: ", partMap)

			err := ChangeLeaderPartition(brokerIDMap, partMap, tp)
			if err != nil {
				fmt.Println(err)
			}
		}
		break
	case "trigger-change-leader":
		for _, tp := range topics {
			if tp == "" || tp == " " {
				break
			}
			partMap := GetPartitionsMap(tp)
			fmt.Println("parititon map: ", partMap)

			err := TriggerChangeLeaderPartitions(tp, partMap, brokerIDMap)

			if err != nil {
				fmt.Println(err)
			}
		}
		break
	}

}
