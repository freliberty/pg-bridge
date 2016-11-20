# PostgreSQL Bridge

Simple bridge for [PostgreSQL notifications](https://www.postgresql.org/docs/9.0/static/sql-notify.html)

## Features

  - PostgreSQL → [Amazon SNS](https://aws.amazon.com/sns/)
  - PostgreSQL → [HTTP Webhooks](https://requestb.in/)

  This package also includes:

  - Optional health checks to ensure that this service is operating normally
  - A Dockerfile to easily deploy this service to any docker-friendly cloud provider

## Running this Service

```sh
pg-bridge --conf bridge.json
```

## Example Configuration

```json
{
  "postgres": {
    "url": "postgres://authenticator"
  },
  "routes": [
    "task.create http://requestb.in/1bpu3kl1",
    "task.update arn:aws:sns:us-west-2:1234:somewhere"
  ],
  "health": {
    "port": 5000,
    "path": "/health"
  }
}
```

## TODO

- SQS support

## License

MIT
