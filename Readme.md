# PostgreSQL Bridge

Simple bridge for [PostgreSQL notifications](https://www.postgresql.org/docs/9.0/static/sql-notify.html)

## Bridge Support

  - PostgreSQL → [Amazon SNS](https://aws.amazon.com/sns/)
  - PostgreSQL → [HTTP Webhooks](https://requestb.in/)

## Additional Features

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
    "url": "postgres://user:pass@localhost:5432/database"
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
## Feedback
the PGB_KINESIS_BUFFER_SIZE variable is to set between 1 to 500. our experiance advice you to set at 100.
to less (5) the iterator age will grow up to 83M millisecond (time out stream) if you have more than 1000 notification per second.
over 100 , we don't see difference with 500.

Don't forget to upsize the batchsize in the lambda to 100.
aws lambda update-event-source-mapping --uuid xxxxxx --function-name xxxxx --batch-size 100

see your test

Buffer size | 	batch size (lambda) | 	nb stream | time execution |nb concurent execution
100         |      	10	            |   20000     |   	4’50’’	   |	2
100	        |      100              | 	20000     |   	2’58’’	   |	1
500         |	     100	            |   20000	    |     2’59’’	   |  1
500         |	     500              |  	20000     |	    2’49’’	   |	1



## TODO

- SQS support

## License

MIT
