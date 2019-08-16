package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"

	"github.com/DataDog/kafka-kit/kafkazk"
	"github.com/spf13/cobra"
)

var (
	zkAddr = "dev-kafka-1.svr.tiki.services:2181"
)

func initZooKeeper() (kafkazk.Handler, error) {
	// Suppress underlying ZK client noise.
	log.SetOutput(ioutil.Discard)

	timeout := 250 * time.Millisecond

	zk, err := kafkazk.NewHandler(&kafkazk.Config{
		Connect: zkAddr,
	})

	if err != nil {
		return nil, fmt.Errorf("Error connecting to ZooKeeper: %s", err)
	}

	time.Sleep(timeout)

	if !zk.Ready() {
		return nil, fmt.Errorf("Failed to connect to ZooKeeper %s within %s", zkAddr, timeout)
		os.Exit(1)
	}

	return zk, nil
}

func getBrokerMeta(cmd *cobra.Command, zk kafkazk.Handler, m bool) kafkazk.BrokerMetaMap {
	brokerMeta, errs := zk.GetAllBrokerMeta(m)
	// If no data is returned, report and exit.
	// Otherwise, it's possible that complete
	// data for a few brokers wasn't returned.
	// We check in subsequent steps as to whether any
	// brokers that matter are missing metrics.
	if errs != nil && brokerMeta == nil {
		for _, e := range errs {
			fmt.Println(e)
		}
		os.Exit(1)
	}

	return brokerMeta
}

// getPartitionMeta returns a map of topic, partition metadata
// persisted in ZooKeeper (via an external mechanism*). This is
// primarily partition size metrics data used for the storage
// placement strategy.
func getPartitionMeta(cmd *cobra.Command, zk kafkazk.Handler) kafkazk.PartitionMetaMap {
	partitionMeta, err := zk.GetAllPartitionMeta()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	return partitionMeta
}

// *References to metrics metadata persisted in ZooKeeper, see:
// https://github.com/DataDog/kafka-kit/tree/master/cmd/metricsfetcher#data-structures)

// getPartitionMap returns a map of of partition, topic config
// (particuarly what brokers compose every replica set) for all
// topics specified. A partition map is either built from a string
// literal input (json from off-the-shelf Kafka tools output) provided
// via the ---map-string flag, or, by building a map based on topic
// config found in ZooKeeper for all topics matching input provided
// via the --topics flag.
func getPartitionMap(cmd *cobra.Command, zk kafkazk.Handler) *kafkazk.PartitionMap {
	ms := cmd.Flag("map-string").Value.String()
	switch {
	// The map was provided as text.
	case ms != "":
		pm, err := kafkazk.PartitionMapFromString(ms)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		return pm
	// Build a map using ZooKeeper metadata
	// for all specified topics.
	case len(Config.topics) > 0:
		pm, err := kafkazk.PartitionMapFromZK(Config.topics, zk)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		return pm
	}

	return nil
}

func main() {
	// ZooKeeper init.
	var zk kafkazk.Handler
	var err error
	zk, err = initZooKeeper()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer zk.Close()

	var brokerMeta kafkazk.BrokerMetaMap
	brokerMeta = getBrokerMeta(cmd, zk, withMetrics)

	// Fetch partition metadata.
	var partitionMeta kafkazk.PartitionMetaMap
	partitionMeta = getPartitionMeta(cmd, zk)

	// Build a partition map either from literal map text input or by fetching the
	// map data from ZooKeeper. Store a copy of the original.
	partitionMapIn := getPartitionMap(cmd, zk)
	originalMap := partitionMapIn.Copy()

}
