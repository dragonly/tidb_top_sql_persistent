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
	proxyAddress *string
)

// monkeyCmd represents the monkeyServer command
var monkeyCmd = &cobra.Command{
	Use:   "monkey",
	Short: "Start testing server",
	Long:  `Monkey server will sometimes drop things`,
	Run: func(cmd *cobra.Command, args []string) {
		app.StartMonkeyServer(*proxyAddress)
	},
}

func init() {
	rootCmd.AddCommand(monkeyCmd)

	// Here you will define your flags and configuration settings.

	proxyAddress = monkeyCmd.Flags().String("proxy", ":0", "TCP proxy address of the gRPC server")
}
