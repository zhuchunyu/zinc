module github.com/zinclabs/zinc

go 1.16

require (
	github.com/aws/aws-sdk-go-v2/config v1.18.2
	github.com/aws/aws-sdk-go-v2/service/s3 v1.27.7
	github.com/blugelabs/bluge v0.1.9
	github.com/blugelabs/bluge_segment_api v0.2.0
	github.com/blugelabs/ice v1.0.0
	github.com/blugelabs/query_string v0.3.0
	github.com/bmizerany/assert v0.0.0-20160611221934-b7ed37b82869 // indirect
	github.com/bwmarrin/snowflake v0.3.0
	github.com/dgraph-io/badger/v3 v3.2103.2
	github.com/docker/go-units v0.4.0
	github.com/getsentry/sentry-go v0.13.0
	github.com/gin-contrib/cors v1.4.0
	github.com/gin-contrib/pprof v1.4.0
	github.com/gin-gonic/gin v1.8.1
	github.com/go-ego/gse v0.70.2
	github.com/goccy/go-json v0.9.11
	github.com/joho/godotenv v1.4.0
	github.com/minio/minio-go/v7 v7.0.34
	github.com/prometheus/client_golang v1.13.0
	github.com/pyroscope-io/client v0.3.0
	github.com/robfig/cron/v3 v3.0.1
	github.com/rs/zerolog v1.28.0
	github.com/segmentio/analytics-go/v3 v3.2.1
	github.com/shirou/gopsutil/v3 v3.22.7
	github.com/stretchr/testify v1.8.0
	github.com/swaggo/files v0.0.0-20220610200504-28940afbdbfe
	github.com/swaggo/gin-swagger v1.5.2
	github.com/swaggo/swag v1.8.5
	github.com/zinclabs/go-gin-prometheus v0.1.1
	github.com/zinclabs/wal v1.2.4
	go.etcd.io/bbolt v1.3.6
	go.etcd.io/etcd/client/v3 v3.5.4
	golang.org/x/crypto v0.0.0-20220722155217-630584e8d5aa
	golang.org/x/sync v0.0.0-20220601150217-0de741cfad7f
	golang.org/x/text v0.3.7
)

replace github.com/blugelabs/bluge => github.com/zinclabs/bluge v1.1.5

replace github.com/blugelabs/ice => github.com/zinclabs/ice v1.1.3

replace github.com/blugelabs/bluge_segment_api => github.com/zinclabs/bluge_segment_api v1.0.0
