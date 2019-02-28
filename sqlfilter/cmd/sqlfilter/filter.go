package main

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// FilterRule defines a filtering criteria to apply to a value.
type FilterRule struct {
	Index  int
	Filter func(value string) bool
}

// Filter is a class that filters sql table insert lines according to a specified criteria.
type Filter struct {
	rules   []FilterRule
	lineRe  *regexp.Regexp
	splitRe *regexp.Regexp
}

// NewFilter allocates a new filter
func NewFilter(rules []FilterRule) *Filter {
	filter := new(Filter)
	filter.rules = rules
	filter.lineRe = regexp.MustCompile("INSERT INTO `(.*)` VALUES " + `(\(.*?\)(?:,\(.*?\))*);`)
	filter.splitRe = regexp.MustCompile(`\((.*?)\)`)
	return filter
}

func (f Filter) filterValue(value string) (bool, int) {
	fields := strings.Split(value, ",")
	for _, rule := range f.rules {
		if rule.Index >= len(fields) {
			continue
		}
		v := fields[rule.Index]
		if len(v) > 1 && v[0] == '\'' && v[len(v)-1] == '\'' {
			v = v[1 : len(v)-1]
		}
		if !rule.Filter(v) {
			return false, -1
		}
	}
	ix, err := strconv.Atoi(fields[0])
	if err != nil {
		fmt.Println(value, " Invalid index: ", fields[0])
		return false, -1
	}
	return true, ix
}

// FilterLine filters a sql insert line
func (f Filter) FilterLine(line string) (string, []int, error) {
	m := f.lineRe.FindStringSubmatch(line)
	if m == nil {
		return "", nil, fmt.Errorf("Invalid line")
	}
	vsplit := f.splitRe.FindAllStringSubmatch(m[2], -1)
	values := make([]string, 0, len(vsplit))
	for _, v := range vsplit {
		values = append(values, v[1])
	}
	nvalues := make([]string, 0, len(values))
	indices := make([]int, 0, len(values))
	for i, v := range values {
		if accept, ix := f.filterValue(v); accept {
			nvalues = append(nvalues, vsplit[i][0])
			indices = append(indices, ix)
		}
	}
	nline := fmt.Sprintf("INSERT INTO `%s` VALUES %s;", m[1], strings.Join(nvalues, ","))
	return nline, indices, nil
}
