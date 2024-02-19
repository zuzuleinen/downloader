# downloader

CLI tool to download a file from a URL.

## Running the script

```go
go run main.go

```

## Tests with sequential downloader

```shell
All bytes downloaded & copied in: 18.109658 seconds
All bytes downloaded & copied in: 21.743763 seconds
All bytes downloaded & copied in: 22.210321 seconds
All bytes downloaded & copied in: 15.578542 seconds
All bytes downloaded & copied in: 30.264280 seconds
All bytes downloaded & copied in: 30.686335 seconds
```

## Tests with parallel downloader (n=8)

```shell
All bytes downloaded & copied in: 26.622065 seconds
All bytes downloaded & copied in: 28.587247 seconds
All bytes downloaded & copied in: 30.784564 seconds
```