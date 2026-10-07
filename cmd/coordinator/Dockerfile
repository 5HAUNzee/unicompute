# Build and run the UniCompute coordinator as a tiny static container.
# Works on Render, Fly.io, Railway, or any Docker host:
#
#   docker build -f cmd/coordinator/Dockerfile -t unicompute-coordinator .
#   docker run -p 8080:8080 unicompute-coordinator
#
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /coordinator ./cmd/coordinator

FROM gcr.io/distroless/static-debian12
COPY --from=build /coordinator /coordinator
EXPOSE 8080
ENTRYPOINT ["/coordinator", "--listen", ":8080"]
