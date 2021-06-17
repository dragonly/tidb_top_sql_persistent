/*
Copyright © 2021 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"log"
	"time"

	"github.com/dragonly/tidb_topsql_agent/internal/app"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	queryTarget *string
	workers     *int
	queryNum    *int
	randomQuery *bool
)

// queryCmd represents the query command
var queryCmd = &cobra.Command{
	Use:   "query",
	Short: "query data for test",
	Long:  `This command runs query to test different database targets`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("start to query database")
		now := time.Now()
		switch *queryTarget {
		case "influxdb":
			app.QueryInfluxDB()
		case "tidb":
			dsn := viper.GetString("dsn")
			log.Printf("dsn: %s\n", dsn)
			app.QueryTiDB(dsn, *workers, *queryNum, *randomQuery)
		default:
			log.Fatalf("Unsupported target [%s]\n", *queryTarget)
		}
		log.Printf("total query time: %ds\n", time.Since(now)/time.Second)
	},
}

func init() {
	rootCmd.AddCommand(queryCmd)

	queryTarget = queryCmd.Flags().StringP("target", "t", "tidb", "select the data generation target database, supports tidb|influxdb")
	workers = queryCmd.Flags().Int("workers", 1, "specify how many concurrent workers are executing queries at the same time")
	queryNum = queryCmd.Flags().Int("queries", 1, "specify total queries")
	randomQuery = queryCmd.Flags().Bool("random", false, "randomize generated query parameters")
}
