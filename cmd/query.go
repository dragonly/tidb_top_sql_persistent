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
)

// queryCmd represents the query command
var queryCmd = &cobra.Command{
	Use:   "query",
	Short: "query data for test",
	Long:  `This command runs query to test different database targets`,
	Run: func(cmd *cobra.Command, args []string) {
		log.Println("start query")
		now := time.Now()
		switch *queryTarget {
		case "influxdb":
			app.QueryInfluxDB()
		case "tidb":
			dsn := viper.GetString("dsn")
			log.Printf("dsn: %s\n", dsn)
			app.QueryTiDB(dsn)
		default:
			log.Fatalf("Unsupported target [%s]\n", *queryTarget)
		}
		log.Printf("query time: %dms\n", time.Since(now)/time.Millisecond)
	},
}

func init() {
	rootCmd.AddCommand(queryCmd)

	queryTarget = queryCmd.Flags().StringP("target", "t", "influxdb", "select the data generation target database, supports influxdb|tidb")
}
