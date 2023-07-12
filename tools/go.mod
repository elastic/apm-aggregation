module github.com/elastic/apm-aggregation/tools

go 1.19

require (
	github.com/elastic/go-licenser v0.4.1
	github.com/planetscale/vtprotobuf v0.4.0
	golang.org/x/tools v0.9.3
	honnef.co/go/tools v0.4.3
)

require (
	github.com/BurntSushi/toml v1.2.1 // indirect
	golang.org/x/exp/typeparams v0.0.0-20221208152030-732eee02a75a // indirect
	golang.org/x/mod v0.10.0 // indirect
	golang.org/x/sys v0.8.0 // indirect
	google.golang.org/protobuf v1.28.1 // indirect
)

replace github.com/planetscale/vtprotobuf => github.com/carsonip/vtprotobuf v0.0.0-20230711135402-2cf3150fde0e
