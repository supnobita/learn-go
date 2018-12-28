package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"os/signal"
	"regexp"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	snmptrappath      = "/var/log/snmptrap/snmptrap.log"
	bootstrap_servers = "172.27.11.158:9092"
	topic             = "noc-backbone"
	offset            int64
	isrotate          = false
	trapfile          *os.File
	trapReader        *bufio.Reader
	isreopenfile      = false
)

//const INTERVAL_PERIOD time.Duration = 24 * time.Hour

const HOUR_TO_TICK int = 17
const MINUTE_TO_TICK int = 2

//const SECOND_TO_TICK int = 03

//read log file and init connection to kafka
func init_steps(filepath string, bootstrap_servers string) (*os.File, *os.File, *kafka.Producer) {
	trapfile, err := os.Open(filepath)
	check(err)
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": bootstrap_servers})
	check(err)
	offsetfile, err := os.OpenFile("offset.log", os.O_RDWR|os.O_CREATE, 0644)
	return trapfile, offsetfile, p
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

//every 10 second, this tool will save the offset point which it read from log file
func writeFileOffset(f *os.File, offset int64) {
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, uint64(offset))
	_, err := f.WriteAt(data, 0)
	check(err)
}

//every 10 second, this tool will save the offset point which it read from log file
//this function will read previous offset point
func readResumeOffset() int64 {
	f, err := os.Open("offset.log")
	if err != nil {
		return 0
	}

	data := make([]byte, 8)
	_, err = f.Read(data)
	if err != nil {
		return 0
	}
	offset := int64(binary.LittleEndian.Uint64(data))
	return offset
}

func ReadLine(r *bufio.Reader) (string, error) {
	data, err := r.ReadString('\n')
	//curpos += int64(len(data))
	if err == nil || err == io.EOF {
		if len(data) > 0 && data[len(data)-1] == '\n' {
			data = data[:len(data)-1]
		}
		if len(data) > 0 && data[len(data)-1] == '\r' {
			data = data[:len(data)-1]
		}
		return data, err
		//fmt.Printf("Pos: %d, Read: %s\n", pos, data)
	}
	//incase we get any error which is not eof
	return data, err
}

//save previous position if we get error (error != EOR)
func HandleReadFileError(err error, savePos int64, offsetfile *os.File) error {
	if err != nil {
		if err != io.EOF {
			//save previous position
			return nil
		}
		writeFileOffset(offsetfile, savePos)
		return err
	}
	return nil
}

func SendMessageToKafka(time string, msg string, p *kafka.Producer) {
	word := "{time: \"" + time + "\"" + "message: " + "\"" + msg + "\""
	kafkaMsg := kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(word),
	}
	p.Produce(&kafkaMsg, nil)
}

// func reopenLogFile(trapfile *os.File, r *bufio.Reader, offset int64) bool {
// 	if isreopenfile {
// 		fmt.Println("reopen file " + snmptrappath)
// 		trapfile, err := os.Open(snmptrappath)
// 		if err != nil {
// 			panic(err)
// 		}
// 		trapfile.Seek(0, 0)
// 		r = bufio.NewReader(trapfile)
// 		//change status
// 		isreopenfile = false
// 		return true
// 	}
// 	return true
// }

// func CheckTimeAndRotateLog(isrotate bool, trapfile *os.File, offsetfile *os.File, r *bufio.Reader) bool {
// 	t := time.Now()
// 	h := t.Hour()
// 	m := t.Minute()

// 	if h == HOUR_TO_TICK && m == MINUTE_TO_TICK && isrotate == false {
// 		err := rotatelog(trapfile)
// 		if err == nil {
// 			fmt.Println("Rename old log")
// 			writeFileOffset(offsetfile, 0)
// 			offset = 0
// 			fmt.Println("Changed offset to 0 and wrote to file")
// 			isreopenfile = true
// 			return true
// 		}
// 	} else if h == HOUR_TO_TICK && m == MINUTE_TO_TICK && isrotate == true {
// 		return true
// 	} else {
// 		return false
// 	}
// 	return false
// }

// func CheckTimeAndRotateLog(isrotate bool, trapfile *os.File, offsetfile *os.File, r *bufio.Reader) bool {
// 	t := time.Now()
// 	h := t.Hour()
// 	m := t.Minute()

// 	if h == HOUR_TO_TICK && m == MINUTE_TO_TICK && isrotate == false {
// 		err := rotatelog(trapfile)
// 		if err == nil {
// 			fmt.Println("Rename old log")
// 			writeFileOffset(offsetfile, 0)
// 			offset = 0
// 			//fmt.Println("Changed offset to 0 and wrote to file")
// 			//isreopenfile = true
// 			//return true
// 			os.Exit(0)
// 		}
// 	} else if h == HOUR_TO_TICK && m == MINUTE_TO_TICK && isrotate == true {
// 		return true
// 	} else {
// 		return false
// 	}
// 	return false
// }

// func rotatelog(trapfile *os.File) error {
// 	t := time.Now()
// 	err := trapfile.Close()
// 	if err != nil {
// 		return err
// 	}
// 	err = os.Rename(snmptrappath, snmptrappath+"-"+t.Format("20060102"))
// 	if err != nil {
// 		//reopen file
// 		fmt.Println("reopen file")
// 		trapfile, _ = os.Open(snmptrappath)
// 	}
// 	return err
// }

func ProcessingLog(trapfile *os.File, offsetfile *os.File, p *kafka.Producer) {

	//timestamp format message
	timereg := regexp.MustCompile("[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1]) (2[0-3]|[01][0-9])(:[0-5][0-9]){2}")
	//snmp format message
	snmpmibreg := regexp.MustCompile("^SNMPv2-MIB::sysUpTime")

	for {
		firstmsg, err := ReadLine(trapReader)
		unknowMsgLen := 0
		if err != nil {
			//write previous error and exit
			writeFileOffset(offsetfile, offset)
			break
		}

		if snmptime := timereg.FindString(firstmsg); len(snmptime) != 0 {
			for {
				secondmsg, err := ReadLine(trapReader)
				if err != nil {
					writeFileOffset(offsetfile, offset)
					break
				}

				if snmpmibreg.MatchString(secondmsg) {
					SendMessageToKafka(snmptime, secondmsg, p)
					offset += int64(len(secondmsg)) + 1 + int64(len(firstmsg)) + 1 + int64(unknowMsgLen)
					unknowMsgLen = 0
					break
				} else if timereg.MatchString(secondmsg) {
					snmptime = timereg.FindString(secondmsg)
					//offset discard len of first timestamp, point to the second timestamp message.
					offset += int64(len(firstmsg)) + 1 + int64(unknowMsgLen)
					unknowMsgLen = 0
					//update to 2nd timestamp message
					firstmsg = secondmsg
				} else {
					unknowMsgLen += len(secondmsg) + 1
				}
			}
		} else if snmpmibreg.MatchString(firstmsg) {
			time := time.Now().String()
			SendMessageToKafka(time, firstmsg, p)
			offset += int64(len(firstmsg)) + 1 + int64(unknowMsgLen)
			unknowMsgLen = 0
		} else {
			unknowMsgLen += len(firstmsg)
			offset += int64(unknowMsgLen) + 1
			unknowMsgLen = 0
		}
	}

	// Wait for message deliveries
	p.Flush(15 * 1000)
}

func cleanup(offsetfile *os.File, trapfile *os.File, p *kafka.Producer) {
	p.Flush(15 * 1000)
	writeFileOffset(offsetfile, offset)
	trapfile.Close()
	offsetfile.Close()
}

func main() {

	trapfile, offsetfile, p := init_steps(snmptrappath, bootstrap_servers)

	offset = readResumeOffset()

	//jump to the offset
	_, err := trapfile.Seek(offset, 0)
	check(err)
	trapReader = bufio.NewReader(trapfile)

	defer p.Close()
	defer offsetfile.Close()

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	//handle signal interupt and signal terminate
	tick := time.Tick(10 * time.Second)

	signalc := make(chan os.Signal)
	signal.Notify(signalc, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	for {
		select {
		case <-tick:
			ProcessingLog(trapfile, offsetfile, p)
			//isrotate = CheckTimeAndRotateLog(isrotate, trapfile, offsetfile, trapReader)
			//fmt.Println(time.Now().String() + ": Run transformate log at offset: ")
			fmt.Printf("%s Run Transformate log at offset %v\n", time.Now(), offset)
		case <-signalc:
			cleanup(offsetfile, trapfile, p)
			fmt.Println("Got signal and stop")
			os.Exit(0)
		}
	}
}
