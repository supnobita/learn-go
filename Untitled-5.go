package main

import (
	"fmt"
	"log"
	"os/exec"
	"strings"
)

func main() {
	out, err := exec.Command("ls", "-la").Output()
	if err != nil {
		log.Fatal(err)
	}
	data := strings.Split(string(out), "\n")

	for _, line := range data {
		fmt.Println(line)
	}
}
