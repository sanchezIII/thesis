package org.traffic.traffic_producer;

import com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import io.vertx.config.ConfigRetriever;

import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.traffic.traffic_producer.experimental.api.ApiService;
import org.traffic.traffic_producer.kafka.KafkaService;
import org.traffic.traffic_producer.observations.ObservationService;
import org.traffic.traffic_producer.sensors.SensorService;
import org.traffic.traffic_producer.streams.StreamService;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Slf4j
public final class MainVerticle extends AbstractVerticle {

  private JsonObject config;

  public static void main(String[] args) {
    Launcher.executeCommand("run", MainVerticle.class.getName(), "-cluster");
  }

  @Override
  public void start(Promise<Void> startPromise) {

    val configRetriever = ConfigRetriever.create(vertx);

    log.info("Loading configuration");
    // Deploying services

    configRetriever
      .getConfig()
      .map(
        config -> {
          log.info("Configuration loaded");
          return this.config = config;
        })
      .compose(
        __ -> {
          if (config.getBoolean("start-sensor-service")) {
            vertx.deployVerticle(
              new SensorService(),
              new DeploymentOptions()
                .setWorker(true)
                .setConfig(config.getJsonObject("sensor-service")));
          }
          return Future.succeededFuture();
        })
      .compose(
        __ -> {
          if (config.getBoolean("start-stream-service")) {
            vertx.deployVerticle(
              new StreamService(),
              new DeploymentOptions()
                .setWorker(true)
                .setConfig(config.getJsonObject("stream-service")));
          }
          return Future.succeededFuture();
        })
      .compose(
        __ -> {
          if (config.getBoolean("start-observation-service")) {
            vertx.deployVerticle(
              new ObservationService(),
              new DeploymentOptions()
                .setWorker(true)
                .setConfig(config.getJsonObject("observation-service")));
          }
          return Future.succeededFuture();
        })
      .compose(
        __ -> {
          if (config.getBoolean("start-api-service")) {
            vertx.deployVerticle(
              new ApiService(),
              new DeploymentOptions()
                .setWorker(true)
                .setConfig(config.getJsonObject("api-service")));
          }
          return Future.succeededFuture();
        })
      .compose(
        __ -> {
          if (config.getBoolean("start-kafka-service")) {
            vertx.deployVerticle(
              new KafkaService(),
              new DeploymentOptions()
                .setWorker(true)
                .setConfig(config.getJsonObject("kafka-service")));
          }
          return Future.succeededFuture();
        })
      .compose(
        __ -> {
          if (config.getBoolean("start-iot-service-gateway")) {
            vertx.deployVerticle(
              new IoTServiceGatewayVerticle(),
              new DeploymentOptions()
                .setWorker(true)
                .setConfig(config.getJsonObject("service-gateway")));
          }
          return Future.succeededFuture();
        })
      .compose(__ -> {
//        Map<String, String> config = new HashMap<>();
//        config.put("bootstrap.servers", "localhost:9092");
//        config.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        config.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        config.put("group.id", "my_group");
//        config.put("auto.offset.reset", "earliest");
//        config.put("enable.auto.commit", "false");
//
//        log.info("OK1");
//        KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, config);
//        log.info("OK2");

        return Future.succeededFuture();
      })
      .onSuccess(__ -> startPromise.complete())
      .onFailure(startPromise::fail);
  }
}
