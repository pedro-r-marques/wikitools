package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
)

const maxLineSize = 1 * 1024 * 1024 * 1024

var (
	inputFile       string
	outputFile      string
	pageTitleFilter string
	outIndexFile    string
	indexFilter     string
)

func initFlags() {
	flag.StringVar(&inputFile, "input", "", "Input filename")
	flag.StringVar(&outputFile, "output", "", "Output file")
	flag.StringVar(&pageTitleFilter, "page-filter", "", "Accept page titles from file")
	flag.StringVar(&indexFilter, "index-filter", "", "Index filter")
	flag.StringVar(&outIndexFile, "index-output", "", "Output file with indices")
}

func newFilterFromOpts() *Filter {
	rules := make([]FilterRule, 0)
	rules = makeNamespaceFilter(rules)
	if indexFilter != "" {
		indexSet := loadIndexSet(indexFilter)
		rules = makeIndexFilter(rules, indexSet)
	}

	if pageTitleFilter != "" {
		pageTitles := loadPageTitles(pageTitleFilter)
		rules = makePageTitleFilter(rules, pageTitles)
	}

	filter := NewFilter(rules)
	return filter
}

func processFile(inp, outp, indexOut *os.File) {
	filter := newFilterFromOpts()
	rd := bufio.NewReader(inp)
	wr := bufio.NewWriter(outp)
	defer wr.Flush()
	var indexWr *bufio.Writer
	if indexOut != nil {
		indexWr = bufio.NewWriter(indexOut)
		defer indexWr.Flush()
	}
	scanner := bufio.NewScanner(rd)
	scanner.Buffer(make([]byte, maxLineSize), maxLineSize)
	for scanner.Scan() {
		line := scanner.Text()
		if nline, indices, err := filter.FilterLine(line); err == nil {
			if len(indices) == 0 {
				continue
			}
			wr.WriteString(nline)
			if indexOut != nil {
				for _, ix := range indices {
					indexWr.WriteString(strconv.Itoa(ix))
					indexWr.WriteByte('\n')
				}
			}
		} else {
			wr.WriteString(line)
		}
		wr.WriteByte('\n')
	}
	if err := scanner.Err(); err != nil {
		fmt.Println(err)
	}
}

func main() {
	initFlags()
	flag.Parse()

	if inputFile == "" {
		log.Fatalln("input option must be specified")
	}
	if outputFile == "" {
		log.Fatalln("output option must be specified")
	}

	inp, err := os.Open(inputFile)
	if err != nil {
		log.Fatalln(inputFile, err)
	}
	defer inp.Close()

	outp, err := os.Create(outputFile)
	if err != nil {
		log.Fatalln(outputFile, err)
	}
	defer outp.Close()

	var indexOut *os.File
	if outIndexFile != "" {
		var err error
		indexOut, err = os.Create(outIndexFile)
		if err != nil {
			log.Fatalln(outIndexFile, err)
		}
	}

	processFile(inp, outp, indexOut)
}
