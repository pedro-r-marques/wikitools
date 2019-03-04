package main

import (
	"io/ioutil"
	"testing"

	"gotest.tools/assert"
)

func TestFilter(t *testing.T) {
	line := "INSERT INTO `page` VALUES " +
		"(10,0,'AccessibleComputing','',1,0,0.33167112649574004,'20190116190543','20190105021557',854851586,94,'wikitext',NULL)," +
		`(246424,1,'Euclid\'s_Elements','',0,0,0.22571038977508998,'20190117095503','20190117115115',863650667,41500,'wikitext',NULL),` +
		"(12,0,'Anarchism','',0,0,0.786172332974311,'20190120003623','20190120011642',878871297,198626,'wikitext',NULL)," +
		"(13,0,'AfghanistanHistory','',1,0,0.0621502865684687,'20190116063934','20190105021557',783865149,90,'wikitext',NULL);"
	filterFn := func(value string) bool {
		return value == "Anarchism"
	}
	filter := NewFilter([]FilterRule{FilterRule{Index: 2, Filter: filterFn}})
	result, indices, err := filter.FilterLine(line)
	assert.NilError(t, err)
	assert.Equal(t, result, "INSERT INTO `page` VALUES (12,0,'Anarchism','',0,0,0.786172332974311,'20190120003623','20190120011642',878871297,198626,'wikitext',NULL);")
	assert.DeepEqual(t, indices, []int{12})
}

func TestFilterParentisis(t *testing.T) {
	line := "INSERT INTO `page` VALUES " +
		"(10,0,'AccessibleComputing','',1,0,0.33167112649574004,'20190116190543','20190105021557',854851586,94,'wikitext',NULL)," +
		"(1271290,7,'Apaches_Ba-keitz-ogie,_(The_Yellow_Coyote),_called_Dutchy_Chiricahua_scout_(F19052_DPLW).jpg','',0,0,0.298012287594,'20180619101909','20180619101909',846533886,1314,'wikitext',NULL)," +
		"(26809,0,'StarCraft_(video_game)','',0,0,0.8594902091282329,'20190120003623','20190120020413',875950064,77587,'wikitext',NULL)," +
		"(3375,0,'Love_and_Theft_(Bob_Dylan_album)','',0,0,0.0622300403772202,'20190119185452','20190119192334',877501164,20608,'wikitext',NULL);"
	filterFn := func(value string) bool {
		return value == "StarCraft_(video_game)"
	}
	filter := NewFilter([]FilterRule{FilterRule{Index: 2, Filter: filterFn}})
	_, indices, err := filter.FilterLine(line)
	assert.NilError(t, err)
	assert.DeepEqual(t, indices, []int{26809})
}

func TestExampleLine(t *testing.T) {
	content, err := ioutil.ReadFile("testdata/line.sql")
	assert.NilError(t, err)
	filterFn := func(value string) bool {
		return value == "One_by_One_(Foo_Fighters_album)"
	}
	filter := NewFilter([]FilterRule{FilterRule{Index: 2, Filter: filterFn}})
	_, indices, err := filter.FilterLine(string(content))
	assert.NilError(t, err)
	assert.DeepEqual(t, indices, []int{592269})
}
