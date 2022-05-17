module github.com/talos-systems/go-loadbalancer

go 1.18

replace inet.af/tcpproxy => github.com/smira/tcpproxy v0.0.0-20201015133617-de5f7797b95b

require (
	github.com/stretchr/testify v1.7.1
	github.com/talos-systems/go-retry v0.3.1
	inet.af/tcpproxy v0.0.0-20200125044825-b6bb9b5b8252
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/kr/pretty v0.1.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/sys v0.0.0-20201015000850-e3ed0017c211 // indirect
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
	gopkg.in/yaml.v3 v3.0.0-20200313102051-9f266ea9e77c // indirect
)
