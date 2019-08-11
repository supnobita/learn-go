#!/bin/bash
#this script will set partition offset by date time on kafka topic for specific consumer group
#this tool write for migrate kafka topic to new cluster
#Parameters:
# topic: name of topic need to set offset (string)
# consumer_group: name of consumer group need to be reset offset (string)
# duplicate_time: how long we accept duplicate message by second (int)
# kafka_source: kafka cluster source (bootstrap_server source)
# kafka_dest:   kafka cluster destination (bootstrap_server destination)
#Usage: kafka-reset-offset.sh kafka_source topic consumer_group duplicate_time kafka_dest
#ex: ./kafka-reset-offset.sh staging-kafka-1.svr.tiki.services:9092 catalog.log_event_bus test_kafka_replicator 60 staging-kafka-1.svr.tiki.services:9092
#-------------
if [[ $# -lt 3 ]]; then
    echo "Usage: kafka-reset-offset.sh kafka_source kafka_dest duplicate_time"
fi

kafka_source=$1
kafka_dest=$2
duplicate_time=$3

Set_offset_by_timestamp() {
    local topic=$1
    local consumer_group=$2
    local offset=$3
    local partition=$4

    re='^[0-9]+$'
    if ! [[ $offset =~ $re ]] ; then
        echo "error:Offset is Not a number, value is: $offset" >&2; exit 1
    fi

    if ! [[ $partition =~ $re ]] ; then
        echo "error:Offset is Not a number, value is: $partition" >&2; exit 1
    fi

    #get unixTime of offset message
    unixTime=$(kafka-console-consumer --bootstrap-server $kafka_source --topic $topic --max-messages 1  --property print.timestamp=true --offset $offset --partition $partition | awk '{print $1}' | cut -d ":" -f2)

    if [[ -z "$unixTime" ]]; then
        echo "cannot get TimeStamp of offset $offset message at topic $topic at partition $partition of consumer: $consumer_group"
        exit 1
    fi
    milisecond=${unixTime: -3} #get milisecond, 3 digit after unixTime
    unixTime=$((unixTime/1000 - duplicate_time)) #back to duplicate_time seconds
    isoTime=$(date -d @$unixTime -Iseconds -u)
    isoTime="${isoTime::19}.$milisecond"

    #set offset
    kafka-consumer-groups --bootstrap-server $kafka_dest --group $consumer_group --topic $topic:$partition --to-datetime $isoTime --reset-offsets --execute
    if [[ $? -eq 1 ]]; then
        echo "cannot set offset offset $offset message at topic $topic at partition $partition of consumer: $consumer_group"
    fi
}

# RunSetOffsetOnGroupAndTopic(){

# local topic=$1
# local consumer_group=$2

# kafka-consumer-groups --bootstrap-server $kafka_source --describe --group $consumer_group --verbose | grep $topic | while read line ; do
#     partition=$(echo $line | awk '{print $3}')
#     offset=$(echo $line | awk '{print $4}')

#     echo "Partition: $partition offset: $offset"
#     Set_offset_by_timestamp $topic $consumer_group $offset $partition
# done
# }


#get all topic except special topic start with _ and connect-*
kafka-topics --bootstrap-server staging-kafka-1.svr.tiki.services:9092 --list | grep -i "^[a-z].*" | grep -v "connect-.*" | while read line; do
    #get all consumer group offsets that access specific topic
    kafka-consumer-groups --bootstrap-server staging-kafka-1.svr.tiki.services:9092 --describe --all-groups | awk '{ if ($2=="$line") print $1 $2 $3 $4}' | while read lineOffset; do
        #Set_offset_by_timestamp $lineOffset
        echo $lineOffset
    done
done


