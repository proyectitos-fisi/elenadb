FROM golang:1.22-alpine

COPY  . .

RUN go mod download

RUN go build -o ./elena ./cmd/elenadb

CMD ["./elena", "test.elena"]
