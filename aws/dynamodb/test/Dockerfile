FROM golang:1.15.2-alpine3.12

RUN apk update && apk upgrade && \
    apk --update add git make

WORKDIR /go/src/app/aws/dynamodb

RUN go get -u github.com/cespare/reflex

CMD reflex -s -r '\.go$' go run test/main.go

