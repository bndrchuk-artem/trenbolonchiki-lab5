FROM golang:1.24 AS build

WORKDIR /go/src/github.com/bndrchuk-artem/trenbolonchiki-lab5
COPY . .

RUN go test ./...
ENV CGO_ENABLED=0
RUN go install ./cmd/...

# ==== Final image ====
FROM alpine:latest
RUN apk add --no-cache ca-certificates wget
WORKDIR /opt/practice-4
COPY entry.sh /opt/practice-4/
COPY --from=build /go/bin/* /opt/practice-4/

# Make entry.sh executable
RUN chmod +x /opt/practice-4/entry.sh

# Create data directory for database
RUN mkdir -p /opt/practice-4/out

RUN ls /opt/practice-4
ENTRYPOINT ["/opt/practice-4/entry.sh"]
CMD ["server"]