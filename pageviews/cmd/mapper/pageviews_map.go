package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"hash/adler32"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"
)

func processFile(filename string, shardID, shardCount uint, projectName string, pageCounts map[string]int) {
	fp, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer fp.Close()

	gz, err := gzip.NewReader(fp)
	if err != nil {
		log.Printf("%s: %s", filename, err.Error())
		return
	}
	defer gz.Close()

	ignoreRe := regexp.MustCompile(`^(Help|Draft|Category|File|User|Talk|(S|s)pecial|Wikipedia|Template)(_talk)?:`)

	projectStr := fmt.Sprintf("%s ", projectName)
	inProject := false
	scanner := bufio.NewScanner(gz)
	for scanner.Scan() {
		if !strings.HasPrefix(scanner.Text(), projectStr) {
			if inProject {
				break
			}
			continue
		}
		inProject = true
		pageTitle, count, _ := Decode(scanner.Text())
		csum := adler32.Checksum([]byte(pageTitle))
		if csum%uint32(shardCount) != uint32(shardID) {
			continue
		}
		if ignoreRe.MatchString(pageTitle) {
			continue
		}
		pageCounts[pageTitle] += count
	}
	if err := scanner.Err(); err != nil {
		log.Printf("%s: %s", filename, err.Error())
	}

}

type keyValuePair struct {
	Key   string
	Value int
}

type byValueDesc []keyValuePair

func (a byValueDesc) Len() int           { return len(a) }
func (a byValueDesc) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byValueDesc) Less(i, j int) bool { return a[i].Value > a[j].Value }

func writeCounts(filename string, pageCounts map[string]int) {
	values := make([]keyValuePair, 0, len(pageCounts))
	for k, v := range pageCounts {
		values = append(values, keyValuePair{k, v})
	}
	sort.Sort(byValueDesc(values))

	fp, err := os.Create(filename)
	if err != nil {
		log.Fatalf("Unable to create %s: %s", filename, err.Error())
	}
	defer fp.Close()
	wr := bufio.NewWriter(fp)
	defer wr.Flush()
	for _, kvp := range values {
		wr.WriteString(fmt.Sprintf("%s\t%d\n", kvp.Key, kvp.Value))
	}
}

func main() {
	inputDir := flag.String("input-path", ".", "Input file directory")
	outputDir := flag.String("output-path", ".", "Output file directory")
	projectOpt := flag.String("project", "en", "Wikipedia project code")
	shardCount := flag.Uint("num-shards", 32, "Total number of shards")
	shardID := flag.Int("id", -1, "Shard number [0, num-shards)")
	verbose := flag.Bool("verbose", true, "Display progress information")
	flag.Parse()
	if *shardID == -1 {
		log.Fatalf("shard-id not specified")
	}

	files, err := ioutil.ReadDir(*inputDir)
	if err != nil {
		log.Fatal(err)
	}

	pageCounts := make(map[string]int)
	re := regexp.MustCompile(`pageviews-\d+-\d+\.gz`)
	for _, file := range files {
		if !re.MatchString(file.Name()) {
			continue
		}
		filename := fmt.Sprintf("%s/%s", *inputDir, file.Name())
		if *verbose {
			fmt.Printf("Processing file %s\n", filename)
		}
		processFile(filename, uint(*shardID), *shardCount, *projectOpt, pageCounts)
	}

	if *verbose {
		fmt.Printf("Pages: %d\n", len(pageCounts))
	}
	filename := fmt.Sprintf("%s/pagecounts-%03d-of-%03d", *outputDir, *shardID, *shardCount)
	writeCounts(filename, pageCounts)
}
