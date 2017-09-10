FROM golang:1.9-alpine

RUN apk update && apk upgrade && \
    apk add --no-cache bash git curl

HEALTHCHECK --interval=5s --timeout=3s --retries=3 \
CMD curl -f http://localhost:5000/health || exit 1

WORKDIR /go/src/app
COPY . .

RUN go-wrapper download
RUN go-wrapper install

CMD ["go-wrapper", "run"]

