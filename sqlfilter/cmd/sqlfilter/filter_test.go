package main

import (
	"testing"

	"gotest.tools/assert"
)

func TestFilter(t *testing.T) {
	line := "INSERT INTO `page` VALUES (10,0,'AccessibleComputing','',1,0,0.33167112649574004,'20190116190543','20190105021557',854851586,94,'wikitext',NULL),(12,0,'Anarchism','',0,0,0.786172332974311,'20190120003623','20190120011642',878871297,198626,'wikitext',NULL),(13,0,'AfghanistanHistory','',1,0,0.0621502865684687,'20190116063934','20190105021557',783865149,90,'wikitext',NULL);"
	filterFn := func(value string) bool {
		return value == "Anarchism"
	}
	filter := NewFilter([]FilterRule{FilterRule{Index: 2, Filter: filterFn}})
	result, indices, err := filter.FilterLine(line)
	assert.NilError(t, err)
	assert.Equal(t, result, "INSERT INTO `page` VALUES (12,0,'Anarchism','',0,0,0.786172332974311,'20190120003623','20190120011642',878871297,198626,'wikitext',NULL);")
	assert.DeepEqual(t, indices, []int{12})
}
