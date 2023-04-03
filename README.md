# tdGo

`tdGo` is a Treasure Data REST API SDK for Go, designed to make it easy for developers to interact with Treasure Data API using Go programming language.  
This SDK provides a simple way to authenticate, manage, and access the resources within the Treasure Data platform.

## Features

- Easy authentication with API key
- Customizable HTTP Client and Logger options
- User-Agent header setting

## Installation

To install tdGo, use the go get command:

```bash
go get github.com/mickeey2525/tdGo
```

## Usage
First, import the package in your Go code:

```go
import "github.com/mickeey2525/tdGo"
```

Create a new tdGo client using your Treasure Data API key and base URL:

```go
client, err := tdGo.NewClient("your-api-key", "https://api.treasuredata.com")
if err != nil {
	log.Fatalf("Failed to create tdGo client: %s", err)
}
```

## Customizing the HTTP Client
To use a custom HTTP client, pass the WithHttpClient option when creating the client:

```go
httpClient := resty.New()
httpClient.SetTimeout(10 * time.Second)

client, err := tdGo.NewClient("your-api-key", "https://api.treasuredata.com", tdGo.WithHttpClient(httpClient))
```

## License
This SDK is released under the MIT License. Please see the LICENSE file for more information.

## Documentation
For detailed information on the Treasure Data REST API, please refer to the official [Treasure Data REST API documentation.](https://api-docs.treasuredata.com/pages/td-api/overview/)

## Contributing
Contributions, issues, and feature requests are welcome! Feel free to open a pull request or issue on the GitHub repository.

## Disclaimer
This SDK is not officially supported by Treasure Data. It is provided as-is, and there is no guarantee that it will always work or be up-to-date with the latest features or improvements in the Treasure Data platform.
