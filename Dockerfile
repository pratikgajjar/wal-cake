FROM golang:1.24-bullseye AS builder

WORKDIR /app

RUN curl --proto '=https' --tlsv1.2 -sSf https://just.systems/install.sh | bash -s -- --to /usr/local/bin

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . ./
RUN just build

FROM alpine:3.21.3

WORKDIR /app

COPY --from=builder /app/dist/cake /var/run/cake

# Create user and group
ENV APP_USER=appuser
RUN addgroup -S $APP_USER && adduser -S $APP_USER -G $APP_USER
RUN chown -R $APP_USER:$APP_USER /var/run/cake
USER $APP_USER

CMD ["sh", "-c", "/var/run/cake"]
