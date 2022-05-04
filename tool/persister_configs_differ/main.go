package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/go-graphite/go-carbon/persister"
)

// persister_configs_differ is a tool that we can use to reason the impact of
// the config changes using file list cache and the new and old config files.
//
// usages: ./persister_configs_differ  \
//         -file-list-cache carbonserver-file-list-cache.gzip  \
//         -new-aggregation new-storage-aggregation.conf  \
//         -new-schema new-storage-schemas.conf  \
//         -old-aggregation oldstorage-aggregation.conf  \
//         -old-schema old-storage-schemas.conf

// TODO: consider merging it as a sub-command in go-carbon?
func main() {
	oldSchemaFile := flag.String("old-schema", "", "old schema file")
	newSchemaFile := flag.String("new-schema", "", "new schema file")
	oldAggregationFile := flag.String("old-aggregation", "", "old aggregation file")
	newAggregationFile := flag.String("new-aggregation", "", "new aggregation file")
	printMetrics := flag.Bool("print-metrics", false, "print metrics with inconsistent configs")
	metricsFile := flag.String("file-list-cache", "", "metrics file list cache")

	flag.Parse()

	oldSchemas, err := persister.ReadWhisperSchemas(*oldSchemaFile)
	if err != nil {
		panic(err)
	}
	newSchemas, err := persister.ReadWhisperSchemas(*newSchemaFile)
	if err != nil {
		panic(err)
	}
	oldAggregations, err := persister.ReadWhisperAggregation(*oldAggregationFile)
	if err != nil {
		panic(err)
	}
	newAggregations, err := persister.ReadWhisperAggregation(*newAggregationFile)
	if err != nil {
		panic(err)
	}

	file, err := os.Open(*metricsFile)
	if err != nil {
		panic(err)
	}
	r, err := gzip.NewReader(file)
	if err != nil {
		panic(err)
	}
	scanner := bufio.NewScanner(r)

	schemaChanges := map[string]int{}
	aggregationChanges := map[string]int{}
	for scanner.Scan() {
		entry := scanner.Text()
		if entry == "" || !strings.HasSuffix(entry, ".wsp") {
			continue
		}
		metric := strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(entry, "/"), ".wsp"), "/", ".")

		// log.Printf("metric = %+v\n", metric)
		//
		// counter++
		// if counter%100_000 == 0 {
		// 	fmt.Printf("count = %+v\n", count)
		// }

		oldSchema, _ := oldSchemas.Match(metric)
		newSchema, _ := newSchemas.Match(metric)
		if !oldSchema.Retentions.Equal(newSchema.Retentions) {
			// fmt.Println(oldSchema.RetentionStr + "->" + newSchema.RetentionStr)
			schemaChanges[oldSchema.RetentionStr+"->"+newSchema.RetentionStr] += 1

			if *printMetrics {
				fmt.Printf("%q,%q,%s,%s,%s\n", oldSchema.RetentionStr, newSchema.RetentionStr, oldSchema.Name, newSchema.Name, metric)
			}
		}
		oldAggregation := oldAggregations.Match(metric)
		newAggregation := newAggregations.Match(metric)
		if oldAggregation.AggregationMethod() != newAggregation.AggregationMethod() {
			// fmt.Println(oldSchema.RetentionStr + "->" + newSchema.RetentionStr)
			aggregationChanges[oldAggregation.AggregationMethod().String()+"->"+newAggregation.AggregationMethod().String()] += 1

			if *printMetrics {
				fmt.Printf("%s,%s,%s,%s,%s\n", oldAggregation.AggregationMethod().String(), newAggregation.AggregationMethod().String(), oldAggregation.Name(), newAggregation.Name(), metric)
			}
		}
	}
	fmt.Println("schema-changes")
	for rule, count := range schemaChanges {
		fmt.Println(rule, count)
	}
	fmt.Println("aggregation-changes")
	for rule, count := range aggregationChanges {
		fmt.Println(rule, count)
	}
}
