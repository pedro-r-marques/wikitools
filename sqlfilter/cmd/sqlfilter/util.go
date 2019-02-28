package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
)

func loadIndexSet(filename string) map[int]bool {
	indexSet := make(map[int]bool)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalln(filename, err)
	}
	rd := bufio.NewReader(file)
	scanner := bufio.NewScanner(rd)
	for scanner.Scan() {
		line := scanner.Text()
		ix, err := strconv.Atoi(line)
		if err != nil {
			fmt.Println("Invalid line in ", filename, ": ", line)
			continue
		}
		indexSet[ix] = true
	}
	if err := scanner.Err(); err != nil {
		fmt.Println(filename, err)
	}
	return indexSet
}

func loadPageTitles(filename string) map[string]bool {
	pageTitles := make(map[string]bool)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalln(filename, err)
	}
	rd := bufio.NewReader(file)
	scanner := bufio.NewScanner(rd)
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), "\t")
		if len(fields) == 0 {
			continue
		}
		if len(fields) != 2 {
			fmt.Println("Unexpected line: ", fields)
			continue
		}
		pageTitles[fields[0]] = true
	}
	if err := scanner.Err(); err != nil {
		fmt.Println(filename, err)
	}
	return pageTitles
}

func makeIndexFilter(rules []FilterRule, indexSet map[int]bool) []FilterRule {
	indexFilterFn := func(value string) bool {
		ix, err := strconv.Atoi(value)
		if err != nil {
			fmt.Println("Invalid index value: ", value)
			return false
		}
		return indexSet[ix]
	}
	rules = append(rules, FilterRule{Index: 0, Filter: indexFilterFn})
	return rules
}

func makePageTitleFilter(rules []FilterRule, pageTitles map[string]bool) []FilterRule {
	pageFilterFn := func(value string) bool {
		return pageTitles[value]
	}
	rules = append(rules, FilterRule{Index: 2, Filter: pageFilterFn})
	return rules
}
