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
	rules  []FilterRule
	lineRe *regexp.Regexp
}

// NewFilter allocates a new filter
func NewFilter(rules []FilterRule) *Filter {
	filter := new(Filter)
	filter.rules = rules
	filter.lineRe = regexp.MustCompile("INSERT INTO `(.*)` VALUES " + `(\(.*?\)(?:,\(.*?\))*);`)
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

// split a sequence of values separated by parenthesis: (any),...,(any)
func splitValues(values string) ([]string, error) {
	result := make([]string, 0)
	var start int
	var inQuotes bool
	var open bool
	var escape bool
	var sep int
	for i := 0; i < len(values); i++ {
		if escape {
			escape = false
			continue
		}
		switch values[i] {
		case '\'':
			inQuotes = !inQuotes
		case '\\':
			escape = true
		}
		if inQuotes {
			continue
		}
		switch values[i] {
		case '(':
			if open {
				return nil, fmt.Errorf("open parenthesis: already open")
			}
			if sep != len(result) {
				fmt.Println(result)
				return nil, fmt.Errorf("values: %d, sep: %d, index: %d", len(result), sep, i)
			}
			start = i + 1
			open = true
		case ')':
			result = append(result, values[start:i])
			open = false
		case ',':
			if !open {
				sep++
			}
		}
	}
	return result, nil
}

// FilterLine filters a sql insert line
func (f Filter) FilterLine(line string) (string, []int, error) {
	m := f.lineRe.FindStringSubmatch(line)
	if m == nil {
		return line, nil, nil
	}
	values, err := splitValues(m[2])
	if err != nil {
		return "", nil, err
	}
	nvalues := make([]string, 0, len(values))
	indices := make([]int, 0, len(values))
	for _, v := range values {
		if accept, ix := f.filterValue(v); accept {
			nvalues = append(nvalues, "("+v+")")
			indices = append(indices, ix)
		}
	}
	if len(nvalues) == 0 {
		return "", nil, nil
	}
	nline := fmt.Sprintf("INSERT INTO `%s` VALUES %s;", m[1], strings.Join(nvalues, ","))
	return nline, indices, nil
}
