package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
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
		fmt.Println("Warning: Unmarshall Json error ", err.Error())
		return nil
	}

	partitionMetaData := make(map[string]map[int]int64)

	for _, v := range partitionMetric.Data.Result {
		partID, err := strconv.Atoi(v.Metric.PartitionId)
		if err != nil {
			fmt.Println("Error: Convert partitionID to Int error ", err.Error())
			continue
		}
		if partitionMetaData[v.Metric.Topic] == nil {
			partitionMetaData[v.Metric.Topic] = make(map[int]int64)
		}
		partitionMetaData[v.Metric.Topic][partID] = v.GetValue()
	}
	return partitionMetaData
}

func GetBrokerMetric(prometheusClusterPrefix string) map[string]int64 {
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
		fmt.Println("Warning: Get metric error ", err.Error())
		return nil
	}

	var metric PrometheusMetric

	decoder := json.NewDecoder(resp.Body)

	err = decoder.Decode(&metric)
	if err != nil {
		fmt.Println("Warning: Unmarshall Broker Json metric error ", err.Error())
		return nil
	}

	metaData := make(map[string]int64)
	for _, v := range metric.Data.Result {
		if v.Metric.Instance != "" {
			metaData[v.Metric.Instance] = v.GetValue()
		}
	}
	return metaData
}

func main() {
	partitionMetric := GetPartitionMetric("vdc-kafka")
	brokerMetric := GetBrokerMetric("vdc-kafka")
	for k, v := range partitionMetric {
		for p, ii := range v {
			fmt.Printf("Partition Metric 0: Topic %v, Partition %d, size: %d\n", k, p, ii)
		}
	}

	for k, v := range brokerMetric {
		fmt.Printf("Broker %v avail_disk: %d\n", k, v)
	}
}
