# Copyright © 2021 Li Yilong <liyilongko@gmail.com>

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

.PHONY: all

all: proto

proto:
	@echo "generating go protobuf files"
	protoc -I internal/app/protobuf --go_out=. --go-grpc_out=. internal/app/protobuf/agent.proto
	@echo ""

test:
	@echo "running unit test"
	go test -v ./...
	@echo ""

bench:
	@echo "running benchmark test"
	go test -benchmem -bench=".*" ./...
	@echo ""
