FROM golang:1.15.8
RUN go get golang.org/x/net/html

WORKDIR .
COPY . .

CMD ["go", "run", "src/WebCrawler.go"]