.PHONY: build test test-race bench kvbench lint cluster-up cluster-down local-cluster clean

build:
	CGO_ENABLED=0 go build -o bin/ ./cmd/...

test:
	go test ./...

test-race:
	go test -race -count=1 ./...

bench:           ## Go micro-benchmarks for the engine
	go test -run=^$$ -bench=. -benchmem ./engine/

kvbench: build   ## db_bench-style engine benchmark
	./bin/kvbench -n 500000

lint:
	gofmt -l . | (! grep .) && go vet ./...

local-cluster: build   ## 3 nodes on localhost (logs in ./data/*.log); stop with: pkill -x kvstore
	mkdir -p data
	./bin/kvstore -id n1 -data data/n1 -http 127.0.0.1:8081 -raft 127.0.0.1:7001 -bootstrap > data/n1.log 2>&1 &
	sleep 1
	./bin/kvstore -id n2 -data data/n2 -http 127.0.0.1:8082 -raft 127.0.0.1:7002 -join http://127.0.0.1:8081 > data/n2.log 2>&1 &
	./bin/kvstore -id n3 -data data/n3 -http 127.0.0.1:8083 -raft 127.0.0.1:7003 -join http://127.0.0.1:8081 > data/n3.log 2>&1 &
	sleep 2 && ./bin/kvctl -addr http://127.0.0.1:8081 status

cluster-up:
	docker compose -f deploy/docker-compose.yml up --build -d

cluster-down:
	docker compose -f deploy/docker-compose.yml down -v

clean:
	rm -rf bin data
