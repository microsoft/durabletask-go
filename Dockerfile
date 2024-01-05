# syntax=docker/dockerfile:1

FROM golang:1.21

COPY . /root
WORKDIR /root

# Download Go modules
COPY go.mod go.sum ./
RUN go mod download

# Build
RUN CGO_ENABLED=0 GOOS=linux go build -o /durabletask-go

EXPOSE 4001

# Run
ENTRYPOINT [ "/durabletask-go" ]
CMD [ "--ip", "0.0.0.0" ]