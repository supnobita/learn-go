package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	zkclient "github.com/samuel/go-zookeeper/zk"
)

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
	zks          = "staging-kafka-1.svr.tiki.services:2181"
	brokerCount  = 0
	throttle     int64
	bumpingSpeed = false
	metricPrefix string
	diskSize     int64
	//topicStatus = 1 is reassign= done, swap leader = 2,
	topicStatus    map[string]int
	statusFileName = "topic-status.json"
)

type PrometheusMetric struct {
	Status string               `json:"status"`
	Data   PrometheusMetricData `json:"data"`
}

type PrometheusMetricData struct {
	ResultType string                   `json:"resultType"`
	Result     []PrometheusMetricResult `json:"result"`
}

type PrometheusMetricResult struct {
	Metric PrometheusObjectMetric `json:"metric"`
	Value  []interface{}          `json:"value"`
}

func (p PrometheusMetricResult) GetValue() int64 {
	if len(p.Value) < 2 {
		return 0
	}
	secondVal, _ := strconv.ParseInt(p.Value[1].(string), 10, 64)
	return secondVal
}

type PrometheusObjectMetric struct {
	TopicPartitionMetric
	BrokerMetric
}

type TopicPartitionMetric struct {
	Topic       string `json:"topic"`
	PartitionId string `json:"partition"`
}

type BrokerMetric struct {
	Instance   string `json:"instance"`
	MountPoint string `json:"mountpoint"`
}

func GetPartitionMetric(prometheusClusterPrefix string) map[string]map[int]int64 {
	MetricUrl := "https://prometheus.infra.tiki.services/api/v1/query"

	req, err := http.NewRequest("GET", MetricUrl, nil)
	//kafka_log_log_size{job=~"vdc-kafka"}
	q := req.URL.Query()
	query := fmt.Sprintf(`kafka_log_log_size{job=~"%v"}`, prometheusClusterPrefix)
	q.Add("query", query)
	req.URL.RawQuery = q.Encode()
	client := http.Client{}
	resp, err := client.Do(req)

	var partitionMetric PrometheusMetric

	partDecoder := json.NewDecoder(resp.Body)

	err = partDecoder.Decode(&partitionMetric)
	if err != nil {
		log.Println("Warning: Unmarshall Json error ", err.Error())
		return nil
	}

	partitionMetaData := make(map[string]map[int]int64)

	for _, v := range partitionMetric.Data.Result {
		partID, err := strconv.Atoi(v.Metric.PartitionId)
		if err != nil {
			log.Println("Error: Convert partitionID to Int error ", err.Error())
			continue
		}
		if partitionMetaData[v.Metric.Topic] == nil {
			partitionMetaData[v.Metric.Topic] = make(map[int]int64)
		}
		partitionMetaData[v.Metric.Topic][partID] = v.GetValue()
	}
	return partitionMetaData
}

func GetBrokerMetric(prometheusClusterPrefix string) map[int]int64 {
	MetricUrl := "https://prometheus.infra.tiki.services/api/v1/query"

	req, err := http.NewRequest("GET", MetricUrl, nil)
	//kafka_log_log_size{job=~"vdc-kafka"}
	//query = 'node_filesystem_avail_bytes{instance=~"vdc-kafka-.*",mountpoint="/",fstype!="rootfs"}'
	q := req.URL.Query()
	query := fmt.Sprintf(`node_filesystem_avail_bytes{instance=~"%v.*",mountpoint="/",fstype!="rootfs"}`, prometheusClusterPrefix)
	q.Add("query", query)
	req.URL.RawQuery = q.Encode()
	client := http.Client{}
	resp, err := client.Do(req)

	if err != nil {
		log.Println("Warning: Get metric error ", err.Error())
		return nil
	}

	var metric PrometheusMetric

	decoder := json.NewDecoder(resp.Body)

	err = decoder.Decode(&metric)
	if err != nil {
		log.Println("Warning: Unmarshall Broker Json metric error ", err.Error())
		return nil
	}

	metaData := make(map[int]int64)
	for _, v := range metric.Data.Result {
		if v.Metric.Instance != "" {
			tempdata := strings.Split(v.Metric.Instance, "-")
			brokerID, err := strconv.Atoi(tempdata[len(tempdata)-1])
			if err != nil {
				log.Println("Error: Convert brokerID error")
				continue
			}
			metaData[brokerID] = v.GetValue()
		}
	}
	return metaData
}

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
func GetRunningAssignment() (PartitionMap, error) {
	c, _, err := zkclient.Connect([]string{zks}, time.Second) //*10)
	if err != nil {
		panic(err)
	}
	path := "/admin/reassign_partitions"

	r, _, err := c.Get(path)
	defer c.Close()
	if err != nil {
		switch err {
		case zkclient.ErrNoNode:
			return PartitionMap{}, err
		default:
			log.Println("Error: ", err.Error())
			return PartitionMap{}, err
		}
	}

	var reassign PartitionMap

	json.Unmarshal(r, &reassign)
	return reassign, nil
}

//get current partition map
//./kafka-reassign-partitions.sh --zookeeper staging-kafka-1.svr.tiki.services:2181 --topics-to-move-json-file test/poc-reassign-topic-request.json --generate --broker-list 1,2,3
func GetPartitionsMap(topic string) PartitionMap {

	if topic == "" {
		log.Println("Topic is empty")
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
		log.Println("Convert Json Partition Map error")
		return PartitionMap{}
	}

	err = ioutil.WriteFile(topic+"-current-map.json", []byte(data[1]), 0644)

	if err != nil {
		log.Fatal(err)
		log.Println("Write file current-map Error")
		return PartitionMap{}
	}

	return partMap

}

func verifyReassignmentStatus(topic_json_file string, newPartitionMap PartitionMap) (bool, error) {

	out, _ := exec.Command("bash", "kafka-reassign-partitions.sh", "--zookeeper", zks,
		"--reassignment-json-file", topic_json_file, "--verify").Output()
	if strings.Count(string(out), "successfully") == len(newPartitionMap.Partitions) {
		return true, nil
	}

	if strings.Contains(string(out), "failed") == true {
		return false, errors.New("ERROR: fatal- failed to reassign partition")
	}

	return false, nil
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
	defer f.Close()
	//save the map to file
	execMapData, err := json.Marshal(newPartitionMap)
	_, err = f.Write(execMapData)
	//TODO: remove return in here
	log.Printf("Map after change: %v\n", newPartitionMap)

	out, err := exec.Command("bash", "kafka-reassign-partitions.sh", "--zookeeper", zks,
		"--reassignment-json-file", topic+"-exec-map.json", "--throttle", strconv.FormatInt(throttle, 10), "--execute").Output()

	if err != nil {
		log.Fatal(err)
		log.Println("Execute Reassign Has Errors")
		return err
	}

	log.Println(string(out))

	for {

		success, err := verifyReassignmentStatus(topic+"-exec-map.json", newPartitionMap)

		if success {
			break
		}
		if err != nil {
			//exit and return error
			return err
		}

		log.Printf("Not yet finish reassign %s with throttle bw %d, Sleep 1p\n", topic, throttle)
		time.Sleep(time.Second * 30) //sleep 30s

		//check time in 00:00 -> 6h, bump speed, if not down speed
		nHours := time.Now().Hour()
		if bumpingSpeed == false && nHours >= 0 && nHours <= 5 {
			//increase speed
			throttle = throttle * 3
			output, err := exec.Command("bash", "kafka-reassign-partitions.sh", "--zookeeper", zks,
				"--reassignment-json-file", topic+"-exec-map.json", "--throttle", strconv.FormatInt(throttle, 10), "--execute").Output()
			if strings.Contains(string(output), "failed") == true || err != nil {
				return errors.New("ERROR: fatal- Throttle Speed error")
			}
			bumpingSpeed = true
		}
		if bumpingSpeed == true && nHours >= 5 {
			throttle = throttle / 3
			output, err := exec.Command("bash", "kafka-reassign-partitions.sh", "--zookeeper", zks,
				"--reassignment-json-file", topic+"-exec-map.json", "--throttle", strconv.FormatInt(throttle, 10), "--execute").Output()
			if strings.Contains(string(output), "failed") == true || err != nil {
				return errors.New("ERROR: fatal- Throttle Speed error")
			}
			bumpingSpeed = false
		}
	}

	log.Printf("-----Success: TOPIC %v reassign successfully---------\n", topic)
	return nil
}

func AddNewSlaveReplica(brokerIds []int, currentPartitionMap PartitionMap) (PartitionMap, error) {
	if len(currentPartitionMap.Partitions) == 0 {
		return PartitionMap{}, errors.New("Warning: Partition map is null")
	}
	if len(currentPartitionMap.Partitions[0].Replicas) >= 3 {
		return PartitionMap{}, errors.New("Warning: Number of replicas of this topic >= 3, please manually handle it !")
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
		log.Printf("Warning: topic %v has added new Broker in Replicas \n", currentPartitionMap.Partitions[0].Topic)
		return PartitionMap{}, errors.New("Warning: topic " + currentPartitionMap.Partitions[0].Topic + " has added new Broker in Replicas")
	}
	var allPartitionsMeta map[string]map[int]int64
	var brokerAvaiDisk map[int]int64
	isMetricEnable := false
	if metricPrefix != "" || len(metricPrefix) != 0 {

		allPartitionsMeta := GetPartitionMetric(metricPrefix)
		brokerAvaiDisk := GetBrokerMetric(metricPrefix)

		if allPartitionsMeta == nil || brokerAvaiDisk == nil {
			log.Printf("ERROR: Cannot get metric")
			return PartitionMap{}, errors.New("Cannot get Broker and Partition metric")
		}
		isMetricEnable = true
	}

	numberOfBroker := len(brokerIds)

	for i, p := range currentPartitionMap.Partitions {
		tryValue := 0
		if isMetricEnable {
			tryValue = choseBrokerByCompareStorage(p, brokerIds, allPartitionsMeta, brokerAvaiDisk, brokerCount)
		} else {
			tryValue = brokerIds[brokerCount%numberOfBroker]
		}
		p.Replicas = append(p.Replicas, tryValue)
		currentPartitionMap.Partitions[i] = p
		brokerCount++
	}

	return currentPartitionMap, nil
}

//chose broker that has free capacity > 0.29
func choseBrokerByCompareStorage(p Partition, brokerIds []int, partmetric map[string]map[int]int64, brokermetric map[int]int64, brokerCount int) int {

	tryValue := -1
	brokerIndex := 0
	numberOfBroker := len(brokerIds)
	for brokerIndex := 0; brokerIndex < len(brokerIds); brokerIndex++ {
		tryValue = brokerIds[(brokerCount)%numberOfBroker]
		freeDisk := brokermetric[tryValue]
		if (float64((freeDisk - partmetric[p.Topic][p.Partition])) / float64(1024*1024*1024*diskSize)) >= 0.29 {
			break // get current tryValue
		}
		brokerCount++
	}
	//if cannot find free broker (broker whose total free disk less than 0.28 percent) ==> raise ERROR and return
	if brokerIndex >= len(brokerIds) {
		log.Println("Error: Cannot find Broker with enough disk")
		return -1
	}
	return tryValue
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
			//log.Print("Warnning: None new brokder ids in list of replicas of topic " + topic + " Manually Handle")
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
		log.Println("Warning: No new brokers in partition replicas, Take a look")
	}
	//generate json file
	for i := range part_map.Partitions {
		tempTopicPart := TopicPartion{Topic: topic, Partition: i}
		preferedPart.Partitions = append(preferedPart.Partitions, tempTopicPart)
	}

	f, err := os.OpenFile("/tmp/topic-prefered-leader-part.json", os.O_RDWR|os.O_CREATE, 0644)
	defer f.Close()
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
		log.Println("ERROR: Execute Preferred Replica Election Has Errors")
		return err
	}
	log.Printf("Topic %v: %v\n", topic, string(out))
	return nil
}

func WriteTopicTrackingStatus(filename string) {

	byteData, err := json.Marshal(topicStatus)

	if err != nil {
		log.Println("Error: write status tracking error ", err.Error())
	}

	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		log.Println("Error: write status tracking error ", err.Error())
	}
	defer f.Close()
	//save the map to file
	_, err = f.Write(byteData)
}

func ReadTopicTrackingStatus(file string) map[string]int {
	dat, err := ioutil.ReadFile(file)
	if err != nil {
		log.Println("Error: Read status tracking ", err.Error())
	}
	m := make(map[string]int)
	json.Unmarshal(dat, &m)
	return m
}

func ReadExecutingMap(file string) (PartitionMap, error) {

	dat, err := ioutil.ReadFile(file)
	if err != nil {
		log.Println("Error: Read Executing map ", err.Error())
		return PartitionMap{}, err
	}
	var result PartitionMap
	err = json.Unmarshal(dat, &result)
	if err != nil {
		log.Println("Error: Load the Executing map ", err.Error())
		return PartitionMap{}, err
	}
	return result, nil

}

func main() {

	flag.StringVar(&zks, "zookeeper", "staging-kafka-1.svr.tiki.services:2181", "Zookeeper url")
	var listBrokers string
	flag.StringVar(&listBrokers, "new-brokers", "", "List new Brokers")
	var ptopic string
	flag.StringVar(&ptopic, "topic", "", "Topic to change (accept regex)")
	var cmd string
	flag.StringVar(&cmd, "cmd", "", "List command, must be: reassign,swapleader,trigger-change-leader")
	flag.Int64Var(&throttle, "throttle", 8000000, "Bandwith throttle")
	flag.StringVar(&metricPrefix, "metrixPrefix", "", "Prometheus metric prefix of kafka cluster")
	flag.Int64Var(&diskSize, "disksize", 512, "Total Disk capacity of kafka Broker (GB)")

	flag.Parse()

	var brokerIds []int
	brokerIDMap := make(map[int]int)

	for _, id := range strings.Split(listBrokers, ",") {
		//support range broker
		if strings.Contains(id, "-") {
			brokerRange := strings.Split(id, "-")
			startID, err := strconv.Atoi(brokerRange[0])
			endID, err2 := strconv.Atoi(brokerRange[1])
			if err == nil || err2 == nil {
				for i := startID; i <= endID; i++ {
					brokerIds = append(brokerIds, i)
				}
			} else {
				log.Println("Parse Broker list Error ", err.Error())
				os.Exit(1)
			}
		} else {
			v, err := strconv.Atoi(id)
			if err != nil {
				log.Println("Parse Broker list Error ", err.Error())
				os.Exit(1)
			}
			brokerIds = append(brokerIds, v)
			brokerIDMap[v] = 1
		}
	}

	topics := GetTopics(ptopic)
	log.Println(topics)
	//get Topic Tracking status
	topicStatus = ReadTopicTrackingStatus(statusFileName)

	switch cmd {
	case "reassign":
		//Wait until the previous reassignment finish
		//get Previous PartitionMap
		previousPart, err := GetRunningAssignment()
		if err == nil || len(previousPart.Partitions) != 0 {
			//test
			fmt.Println(previousPart)
			pData, _ := json.Marshal(previousPart)
			ioutil.WriteFile("/tmp/test-topic.json", pData, 0644)

			for {
				success, err := verifyReassignmentStatus("/tmp/test-topic.json", previousPart)
				if success {
					break
				} else if err != nil {
					log.Fatal("Error when verify previous Reassign process " + err.Error())
				} else {
					log.Println("INFO: Wait for previous Reassign Process finish")
					time.Sleep(time.Second * 30) //sleep 30s
				}
			}
		}

		for _, tp := range topics {
			if tp == "" || tp == " " {
				log.Println("Warning: Empty topic name will be omit")
				continue
			}

			//if topicstatus is 1 or 2 omit that topic
			if topicStatus[tp] == 1 || topicStatus[tp] == 2 {
				continue
			}

			partMap := GetPartitionsMap(tp)
			log.Println("parititon map: ", partMap)
			newPartitionMap, err := AddNewSlaveReplica(brokerIds, partMap)
			if err != nil {
				log.Print(err)
				continue // omit RunAssigPartition task
			}
			err = RunAssignPartition(tp, newPartitionMap)
			if err != nil {
				panic(err)
			}
			topicStatus[tp] = 1 //reassignment done
			WriteTopicTrackingStatus(statusFileName)
		}
		break
	case "swapleader":
		for _, tp := range topics {
			if tp == "" || tp == " " {
				log.Println("Warning: Empty topic name will be omit")
				continue
			}

			partMap := GetPartitionsMap(tp)
			log.Println("parititon map: ", partMap)

			err := ChangeLeaderPartition(brokerIDMap, partMap, tp)
			if err != nil {
				log.Println(err)
			}

			topicStatus[tp] = 2 //finish swap leader
			WriteTopicTrackingStatus(statusFileName)
		}
		break
	case "trigger-change-leader":
		for _, tp := range topics {
			if tp == "" || tp == " " {
				break
			}
			partMap := GetPartitionsMap(tp)
			log.Println("parititon map: ", partMap)

			err := TriggerChangeLeaderPartitions(tp, partMap, brokerIDMap)

			if err != nil {
				log.Println(err)
			}
		}
		break
	default:
		log.Println("Command not found !!! support: reassign,swaptrigger,trigger-change-leader")
	}

}
