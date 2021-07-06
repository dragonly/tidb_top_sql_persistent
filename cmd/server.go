/*
Copyright © 2021 Li Yilong <liyilongko@gmail.com>

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
	"os"
	"strconv"
	"time"

	"github.com/spf13/cobra"

	"github.com/dragonly/tidb_topsql_agent/internal/app"
)

var (
	storeType *string
	address   *string
)

// serverCmd represents the serve command
var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "start agent server",
	Long:  `This will start the gRPC server of the data collection agent.`,
	Run: func(cmd *cobra.Command, args []string) {
		var store app.Store
		switch *storeType {
		case "memstore":
			store = app.NewMemStore()
		case "tidb":
			dsn := os.Getenv("TIDB_DSN")
			clusterIDStr := os.Getenv("CLUSTER_ID")
			clusterID, err := strconv.ParseUint(clusterIDStr, 10, 64)
			if err != nil {
				log.Fatalf("cannot convert CLUSTER_ID(\"%s\") to integer", clusterIDStr)
			}
			log.Printf("TIDB_DSN: \"%s\"\n", dsn)
			store = app.NewTiDBStore(dsn, clusterID)
			if err := store.InitSchema(); err != nil {
				log.Fatalf("initialize TiDB schema failed: %v", err)
			}
		default:
			log.Fatalf("invalid store type: %v", *storeType)
		}
		app.StartGrpcServer(*address, store)
		time.Sleep(time.Hour * 24 * 365)
	},
}

func init() {
	rootCmd.AddCommand(serverCmd)

	storeType = serverCmd.Flags().String("store", "memstore", "select the store implementation, defaults to memstore for test purpose")
	address = serverCmd.Flags().String("address", ":23333", "specify the gRPC server listening address")
}
