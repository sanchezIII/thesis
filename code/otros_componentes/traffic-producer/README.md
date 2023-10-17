
# traffic-producer

[![Vertx](https://img.shields.io/badge/vert.x-4.1.2-purple.svg)](https://vertx.io)
![example workflow](https://github.com/hendo9701/traffic-producer/actions/workflows/main.yml/badge.svg)
![Code Quality](https://www.code-inspector.com/project/28542/score/svg)
![Code Grade](https://www.code-inspector.com/project/28542/status/svg)

## General Description

One of the microservices of the traffic project in charge of collecting information about sensors, streams and
observations from https://data.mobility.brussels/traffic/api/counts/
to later be registered and published

## Configuration

This application can be configured through the ``conf/config.json`` file, although a different configuration file can be
specified indicating the path through the:

1. ``vertx-config-path`` system property or
1. the ``VERTX_CONFIG_PATH`` environment variable.

## How the service works?

Once the service has started, a single request is made to the IoT service mentioned in the general description to obtain
sensor metadata and then derive the corresponding streams. Once this operation finishes a periodic task is scheduled (
whose time can be specified in the configuration file)
that will be in charge of collecting observations from the sensors previously obtained and then publishing them to the
designated message broker.

Each observation is published to the message broker channel whose name matches the **id** of the stream to which it
belongs; therefore, any client application interested in consuming these observations, only has to subscribe to the
corresponding channel.

## Building

#### Running the tests:

``./mvnw clean test``

#### Packaging the application:

``./mvnw clean package``

#### Running the application:

``./mvnw clean compile exec:java``
