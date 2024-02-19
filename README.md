# downloader

This script showcases downloading a file over HTTP in parallel or by doing a GET request in Golang.

The file is located at https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2018-05.parquet and has 125MB. In
case this file will be removed at one point, feel free to replace the URL.

## Running the script

To download the file sequentially in 1 GET request:

```go
go run main.go -s
```

To download the file in 8 concurrent requests:

```go
go run main.go -p 8
```

You could also do 1 GET request by spinning only 1 worker, but the reason I have a separate method is to showcase the
difference between a simple GET request and
an [HTTP Range request](https://developer.mozilla.org/en-US/docs/Web/HTTP/Range_requests).

## Contact me for any issues

If you find any issues or suggestions feel free to write me at `andrey.boar@gmail.com` or connect with me
via [Linkedin](https://www.linkedin.com/in/andrei-boar-7aa32ab7/).

## Credits

This is a slightly modified solution for the [Final Exercise](https://www.353solutions.com/c/znga/dld.html) from the
course Practical Go - Foundations organized by [Miki
Tebeka](https://twitter.com/tebeka) on [ardanlabs.com](https://www.ardanlabs.com/)