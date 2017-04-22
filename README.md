commute-api-go
==============

Build Docker image:

```
    docker build -t commute-api .
```

```
    docker run \
            --publish 8080:8080 \
            --name commute-api \
            --rm \
            --env "GIN_MODE=release" \
            --env "REDIS_HOST=192.168.4.10" \
            --env "REDIS_PORT=6379" \
            --env "DB_PROTOCOL=http" \
            --env "DB_HOST=192.168.4.10" \
            --env "DB_PORT=8087" \
            --env "DB_USER=commute" \
            --env "DB_PASSWORD=commute" \
            --env "DB_DATABASE=commute" \
            commute-api-go
```
