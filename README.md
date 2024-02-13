# Datagen / SPITHA Distribution
Datagen Spitha is a project designed to generate messages and perform load testing on Kafka. The main goal is to simulate real-world scenarios by producing a stream of data that Kafka can handle, thereby optimizing the system. The data generated by Datagen is adjusted within the processing capabilities of Kafka.

The distinctive features of Datagen include:

- Support for Kafka basic authentication and AWS_MSK_IAM authentication
- Ability to specify topic partitions and replicas
- Configurable messages per second
- Adjustable message size per case
- Configurable go-routine threads
- Adjustable message creation interval per second
- Ability to specify the desired amount of messages to be produced per second

## Build

Datagen uses Docker to ensure the same build environment. If Docker is installed, you can build it using the following command.

```bash
make build
```

## Quickstart

You can set the following and get started quickly with the command.

``` yaml
services:
  kafka-datagen:
    image: spitharepo/kafka-datagen:latest
    environment:
      BOOTSTRAP_SERVER: localhost:9092
      TOPIC_NAME: datagen-users
      DATAGEN_LIMIT_DATA_AMOUNT_PER_SECOND: 10000
```

```bash
docker-compose -f example/docker-compose.yml  up -d
```

or

```bash
docker run -e BOOTSTRAP_SERVER=localhost:9092 -e TOPIC_NAME=datagen-users -e DATAGEN_LIMIT_DATA_AMOUNT_PER_SECOND=10000 spitharepo/kafka-datagen:latest
```

## Configuration Precautions
- When generating data with Datagen, the following settings should be noted:

### Produce Type (Choose one)
- datagen.limit-data-amount-per-second
  - This setting limits the amount of data per second. It can be used in conjunction with the `quickstart` and `message-bytes` settings, and in case it exceeds the set byte figure, it limits the amount of data per second.
- datagen.interval
  - This setting sets an interval for data transmission. It can be used in conjunction with the `quickstart` and `message-bytes` settings, and it puts an interval between each piece of data.
- datagen.rate-per-second
  - This setting allows you to set the amount of data per second. It can be used in conjunction with the `quickstart` and `message-bytes` settings. If set to 500, for example, it will generate 500 pieces of data.

### Message Type (Choose one)
- datagen.quickstart and datagen.message-bytes
  - Each setting specifies the type of message. In the case of quickstart, it generates messages randomly in the specified format. In the case of message-bytes, it specifies the size of the message to be produced. The two settings are about the data that generates messages and cannot be used simultaneously.

## (Docker) Environment Settings 

### Datagen Producer Settings 
| Configuration                 | Description                                 | Default Value   |
|-------------------------------|---------------------------------------------|-----------------|
| `BOOTSTRAP_SERVER`            | Kafka broker address                        |        -        |
| `PRODUCER_MAX_MESSAGE_BYTES`  | Producer `max.message.bytes` setting        |        -        |
| `PRODUCER_LINGERS`            | Producer `lingers` setting                  |        -        |
| `PRODUCER_COMPRESSION_TYPE`   | Producer compression setting                |        -        |
| `PRODUCER_CLIENT_ID`          | Producer clieit id setting                  |        -        |


### Datagen Producer Authentication


| Configuration                       | Description                                                                   | Default Value   |
|-------------------------------------|-------------------------------------------------------------------------------|-----------------|
| `PRODUCER_SASL_MECHANISM`           | Producer authentication mechanism setting                                     |        -        |
| `PRODUCER_SASL_USERNAME`            | User setting according to SCRAM,PLAIN authentication mechanism                |        -        |
| `PRODUCER_SASL_PASSWORD`            | User password setting according to SCRAM,PLAIN authentication mechanism       |        -        |
| `PRODUCER_SASL_AWS_ACCESS_KEY_ID`   | `aws-access-key-id` setting according to AWS authentication mechanism         |        -        |
| `PRODUCER_SASL_AWS_SECRET_ACCESS_KEY` | `aws-secret-access-key` setting according to AWS authentication mechanism   |        -        |
| `PRODUCER_SASL_CLIENT_ID`           | `client-id` setting according to OAUTHBEARER authentication mechanism         |        -        |
| `PRODUCER_SASL_CLIENT_SECRET`       | `client-secret` setting according to OAUTHBEARER authentication mechanism     |        -        |
| `PRODUCER_SASL_TOKEN_ENDPOINT`      | `token-endpoint` setting according to OAUTHBEARER authentication mechanism    |        -        |


### Datagen Producer Tls
| Configuration                   | Description                             | Default Value   |
|---------------------------------|-----------------------------------------|-----------------|
| `PRODUCER_TLS_CAFILE`           | TLS CAFile registration                 |        -        |
| `PRODUCER_TLS_CERTFILE`         | TLS CertFile registration               |        -        |
| `PRODUCER_TLS_KEYFILE`          | TLS KeyFile registration                |        -        |
| `PRODUCER_TLS_SKIPVERIFY`       | TLS Skipverify setting                  |        -        |


### Topic 
| Configuration Item         | Description                                | Default Value   |
|----------------------------|--------------------------------------------|-----------------|
| `TOPIC_NAME`               | Topic name                                 |        -        |
| `TOPIC_PARTITION`          | Number of partitions (at initial creation) | 3               |
| `TOPIC_REPLICA_FACTOR`     | Number of replicas (at initial creation)   | 1               |


### Datagen
| Configuration                             | Description                                                                          | Default Value   |
|-------------------------------------------|--------------------------------------------------------------------------------------|-----------------|
| `DATAGEN_QUICKSTART`                      | Data generation `quickstart` setting (user, book, car, address, contact, movie, job) |        -        |
| `DATAGEN_MESSAGE_BYTES`                   | Setting for `message-bytes` generated per entry                                      | 100             |
| `DATAGEN_GO_ROUTINE`                      | Setting for the number of `go-routine`                                               | 1               |
| `DATAGEN_RATE_PER_SECOND`                 | Setting for the number of messages per second in `rate-per-second`                   |        -        |
| `DATAGEN_INTERVAL`                        | Setting for message transmission interval in `interval`                              | 0               |
| `DATAGEN_LIMIT_DATA_AMOUNT_PER_SECOND`    | Adjusting the limit of message amount per second in `limit-data-amount-per-second`   |        -        |


# License
Apache License 2.0, see LICENSE.