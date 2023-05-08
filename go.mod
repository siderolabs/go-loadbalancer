module github.com/siderolabs/go-loadbalancer

go 1.20

replace inet.af/tcpproxy => github.com/smira/tcpproxy v0.0.0-20201015133617-de5f7797b95b

require (
	github.com/siderolabs/go-retry v0.3.2
	github.com/stretchr/testify v1.8.2
	go.uber.org/goleak v1.2.1
	inet.af/tcpproxy v0.0.0-20221017015627-91f861402626
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/sys v0.8.0 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
