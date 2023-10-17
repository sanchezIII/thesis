package org.traffic.traffic_registry;

import io.vertx.config.ConfigRetriever;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Launcher;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.traffic.traffic_registry.sensor.SensorService;
import org.traffic.traffic_registry.stream.StreamService;

@Slf4j
public final class MainVerticle extends AbstractVerticle {

  private JsonObject config;

  public static void main(String[] args) {
    Launcher.executeCommand("run", MainVerticle.class.getName(), "-cluster");
  }

  @Override
  public void start(Promise<Void> startPromise) {

    val configRetriever = ConfigRetriever.create(vertx);

    configRetriever
        .getConfig()
        .map(
            config -> {
              log.info("Config file loaded");
              return this.config = config;
            })
        .compose(
            __ ->
                vertx.deployVerticle(
                    new SensorService(),
                    new DeploymentOptions()
                        .setWorker(true)
                        .setConfig(config.getJsonObject("sensor-service"))))
        .compose(
            __ ->
                vertx.deployVerticle(
                    new StreamService(),
                    new DeploymentOptions()
                        .setWorker(true)
                        .setConfig(config.getJsonObject("stream-service"))))
        .compose(
            __ ->
                vertx.deployVerticle(
                    new RegistryVerticle(),
                    new DeploymentOptions().setConfig(config.getJsonObject("registry"))))
        .onSuccess(
            __ -> {
              log.info("Deployed Main verticle!");
              startPromise.complete();
            })
        .onFailure(startPromise::fail);
  }
}
