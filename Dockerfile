FROM golang:1.23-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -mod=vendor -trimpath -ldflags="-s -w" -o /scheduler ./cmd/server
RUN CGO_ENABLED=0 go build -mod=vendor -trimpath -ldflags="-s -w" -o /worker ./cmd/worker

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /scheduler /scheduler
COPY --from=build /worker /worker
EXPOSE 8090
USER nonroot:nonroot
ENTRYPOINT ["/scheduler"]
