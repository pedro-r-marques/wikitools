package main

import (
	"log"
	"strconv"
	"strings"
)

// Decode a pageviews line
// Format: project.code page_title count bytes(?)
func Decode(line string) (string, int, int) {
	fields := strings.Split(line, " ")
	if len(fields) < 4 {
		log.Fatalln("Invalid input: ", line)
	}
	last, err := strconv.Atoi(fields[len(fields)-1])
	if err != nil {
		log.Print(err)
	}
	count, err := strconv.Atoi(fields[len(fields)-2])
	if err != nil {
		log.Fatal(err)
	}
	pageTitle := strings.Join(fields[1:len(fields)-2], " ")
	return pageTitle, count, last
}
