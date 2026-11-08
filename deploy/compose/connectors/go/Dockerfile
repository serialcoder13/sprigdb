# go-sql-driver/mysql (version pinned in go.mod / go.sum).
FROM golang:1.25.3-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY main.go ./
RUN CGO_ENABLED=0 go build -trimpath -o /check .

FROM alpine:3
COPY --from=build /check /check
ENTRYPOINT ["/check"]
