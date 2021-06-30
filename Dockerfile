FROM golang:alpine as builder
WORKDIR /workspace

COPY go.mod /workspace/go.mod
COPY go.sum /workspace/go.sum
RUN go mod download

COPY main.go /workspace/main.go
COPY ./cmd /workspace/cmd
COPY ./internal /workspace/internal

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o tidb_topsql_agent


FROM alpine
COPY --from=builder /workspace/tidb_topsql_agent /app/tidb_topsql_agent
RUN chmod +x /app/tidb_topsql_agent
ENTRYPOINT [ "/app/tidb_topsql_agent" ]
CMD [ "server" ]
