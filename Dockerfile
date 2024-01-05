# syntax=docker/dockerfile:1

FROM golang:1.21

COPY . /root
WORKDIR /root

# Download Go modules
COPY go.mod go.sum ./
RUN go mod download

# Build
RUN CGO_ENABLED=0 GOOS=linux go build -o /durabletask-go

# To bind to a TCP port, runtime parameters must be supplied to the docker command.
# But we can (optionally) document in the Dockerfile what ports
# the application is going to listen on by default.
# https://docs.docker.com/engine/reference/builder/#expose
EXPOSE 4001

# Run
ENTRYPOINT [ "/durabletask-go" ]