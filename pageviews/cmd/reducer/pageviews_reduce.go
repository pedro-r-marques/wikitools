package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

func hasInput(cursors []string) bool {
	for _, str := range cursors {
		if str != "" {
			return true
		}
	}
	return false
}

type mergeStreams struct {
	scanners []*bufio.Scanner
	values   []string
	output   *os.File
}

func newMergeStreams(fileset []*os.File, output *os.File) *mergeStreams {
	s := new(mergeStreams)
	s.scanners = make([]*bufio.Scanner, len(fileset))
	for i, file := range fileset {
		s.scanners[i] = bufio.NewScanner(file)
	}
	s.values = make([]string, len(fileset))
	s.output = output
	return s
}

func (s mergeStreams) Len() int {
	return len(s.scanners)
}

func (s mergeStreams) Fill(index int) (bool, int) {
	scanner := s.scanners[index]
	if !scanner.Scan() {
		return false, -1
	}
	s.values[index] = scanner.Text()
	fields := strings.Split(s.values[index], "\t")
	v, err := strconv.Atoi(fields[len(fields)-1])
	if err != nil {
		log.Fatal(err)
	}
	return true, v
}

func (s mergeStreams) Commit(index int) {
	s.output.WriteString(s.values[index])
	s.output.WriteString("\n")
}

func (s mergeStreams) Check() {
	for _, scanner := range s.scanners {
		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}
	}
}

func mergeSort(fileset []*os.File, filename string, limit int) {
	file, err := os.Create(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	streams := newMergeStreams(fileset, file)
	MergeStreams(streams)
	streams.Check()
}

func main() {
	inputDir := flag.String("input-path", ".", "Input file directory")
	outputDir := flag.String("output-path", ".", "Output file directory")
	shardCount := flag.Int("num-shards", 32, "Total number of shards")
	limitOpt := flag.Int("limit", -1, "")
	flag.Parse()

	fileset := make([]*os.File, *shardCount)
	for i := 0; i < *shardCount; i++ {
		filename := fmt.Sprintf("%s/pagecounts-%03d-of-%03d", *inputDir, i, *shardCount)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal(err)
		}
		fileset[i] = file
	}
	filename := fmt.Sprintf("%s/pagecounts-summary.tsv", *outputDir)
	mergeSort(fileset, filename, *limitOpt)
	for _, file := range fileset {
		file.Close()
	}
}
