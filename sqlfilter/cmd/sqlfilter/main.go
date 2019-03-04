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
	optInputFile       string
	optOutputFile      string
	optPageTitleFilter string
	optOutputIndexFile string
	optIndexFilter     string
	chunkSize          int
	skipChunks         int
)

func initFlags() {
	flag.StringVar(&optInputFile, "input", "", "Input filename")
	flag.StringVar(&optOutputFile, "output", "", "Output file")
	flag.StringVar(&optPageTitleFilter, "page-filter", "", "Accept page titles from file")
	flag.StringVar(&optIndexFilter, "index-filter", "", "Index filter")
	flag.StringVar(&optOutputIndexFile, "index-output", "", "Output file with indices")
	flag.IntVar(&chunkSize, "chunk-size", 0, "Number of lines per chunk")
	flag.IntVar(&skipChunks, "skip", 0, "Ignore the first n chunks of the input file")
}

func newFilterFromOpts() *Filter {
	rules := make([]FilterRule, 0)
	rules = makeNamespaceFilter(rules)
	if optIndexFilter != "" {
		indexSet := loadIndexSet(optIndexFilter)
		rules = makeIndexFilter(rules, indexSet)
	}

	if optPageTitleFilter != "" {
		pageTitles := loadPageTitles(optPageTitleFilter)
		rules = makePageTitleFilter(rules, pageTitles)
	}

	filter := NewFilter(rules)
	return filter
}

type fileManager struct {
	outputFile  *os.File
	indexFile   *os.File
	writeStream *bufio.Writer
	indexStream *bufio.Writer
	chunkIndex  int
}

func newFileManager() *fileManager {
	fm := new(fileManager)
	fm.chunkIndex = -1
	return fm
}

func (fm *fileManager) getFilenames(current int) (string, string) {
	if chunkSize > 0 {
		var outputFile, indexFile string
		outputFile = fmt.Sprintf("%s.%d", optOutputFile, current)
		if optOutputIndexFile != "" {
			indexFile = fmt.Sprintf("%s.%d", optOutputIndexFile, current)
		}
		return outputFile, indexFile
	}
	return optOutputFile, optOutputIndexFile
}

func (fm *fileManager) GetOutputStreams(current int) (wr, indexWr *bufio.Writer) {
	if fm.chunkIndex != current {
		fm.Close()

		outputFile, indexFile := fm.getFilenames(current)

		var err error
		fm.outputFile, err = os.Create(outputFile)
		if err != nil {
			log.Fatalln(outputFile, err)
		}

		fm.writeStream = bufio.NewWriter(fm.outputFile)

		if indexFile != "" {
			var err error
			fm.indexFile, err = os.Create(indexFile)
			if err != nil {
				log.Fatalln(indexFile, err)
			}
			fm.indexStream = bufio.NewWriter(fm.indexFile)
		}
		fm.chunkIndex = current
	}
	return fm.writeStream, fm.indexStream
}

func (fm *fileManager) Close() {
	if fm.writeStream != nil {
		fm.writeStream.Flush()
	}
	if fm.indexStream != nil {
		fm.indexStream.Flush()
	}
	if fm.outputFile != nil {
		fm.outputFile.Close()
	}
	if fm.indexFile != nil {
		fm.indexFile.Close()
	}
}

func processFile(inp *os.File) {
	filter := newFilterFromOpts()
	rd := bufio.NewReader(inp)
	scanner := bufio.NewScanner(rd)
	scanner.Buffer(make([]byte, maxLineSize), maxLineSize)
	var lineCounter, chunkIndex int
	fm := newFileManager()
	defer fm.Close()
	for scanner.Scan() {
		line := scanner.Text()
		if chunkSize > 0 {
			chunkIndex = lineCounter / chunkSize
		}
		lineCounter++
		if chunkIndex < skipChunks {
			continue
		}
		wr, indexWr := fm.GetOutputStreams(chunkIndex)
		nline, indices, err := filter.FilterLine(line)
		if err != nil {
			log.Fatalf("line %d: %s", lineCounter, err.Error())
		}
		if nline == "" {
			continue
		}
		wr.WriteString(nline)
		wr.WriteByte('\n')

		if indexWr != nil {
			for _, ix := range indices {
				indexWr.WriteString(strconv.Itoa(ix))
				indexWr.WriteByte('\n')
			}
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Println(err)
	}
}

func main() {
	initFlags()
	flag.Parse()

	if optInputFile == "" {
		log.Fatalln("input option must be specified")
	}
	if optOutputFile == "" {
		log.Fatalln("output option must be specified")
	}

	inp, err := os.Open(optInputFile)
	if err != nil {
		log.Fatalln(optInputFile, err)
	}
	defer inp.Close()

	processFile(inp)
}
