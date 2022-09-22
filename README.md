SQS Example
===============

# Description

Start API:
```shell
make api
```

Start the Worker:
```shell
make worker
```

Create a Job:
```shell
curl localhost:8080/jobs -d'{"message": "this is a job"}'
```

See the Job being consumed by the Worker. 