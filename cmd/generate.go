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
	"github.com/dragonly/tidb_topsql_agent/internal/app"
	"github.com/spf13/cobra"
)

var (
	target *string
)

// generateCmd represents the generate command
var generateCmd = &cobra.Command{
	Use:   "generate",
	Short: "generate data for test",
	Long:  `This command generates mock data for different database targets.`,
	Run: func(cmd *cobra.Command, args []string) {
		switch *target {
		case "influxdb":
			app.WriteInfluxDB()
		case "tidb":
			app.WriteTiDB()
		}
	},
}

func init() {
	rootCmd.AddCommand(generateCmd)

	target = generateCmd.Flags().StringP("target", "t", "influxdb", "select the data generation target database, supports influxdb|tidb")
}
