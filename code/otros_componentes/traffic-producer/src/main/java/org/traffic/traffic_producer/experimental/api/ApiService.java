package org.traffic.traffic_producer.experimental.api;

import io.vertx.config.ConfigRetriever;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.traffic.traffic_producer.sensors.Sensor;
import org.traffic.traffic_producer.streams.Stream;
import org.traffic.traffic_producer.util.JsonCollector;

import java.util.List;

import static org.traffic.traffic_producer.sensors.SensorService.SENSOR_SERVICE_ADDRESS;
import static org.traffic.traffic_producer.streams.StreamService.STREAM_SERVICE_ADDRESS;

@Slf4j
public final class ApiService extends AbstractVerticle {

  public Router createRouter() {
    Router router = Router.router(vertx);

    router.route("/").handler(routingContext -> {
      HttpServerResponse response = routingContext.response();

      log.info("Request to http://localhost:8080/");

      response
        .putHeader("content-type", "text/html")
        .end("Hello World!");
    });

    router.route(Constants.addSensorRoute + "*").handler(BodyHandler.create());
    router.route(Constants.addSensorRoute)
      .handler(routingContext -> {
        Sensor sensor = Sensor.fromJson(routingContext.getBodyAsJson());

        vertx
          .eventBus()
          .request(
            SENSOR_SERVICE_ADDRESS,
            Sensor.asJson(sensor),
            action("save"));

        routingContext.response()
          .setStatusCode(201)
          .putHeader("content-type", "application/json; charset=utf-8")
          .end(Sensor.asJson(sensor).toString());
      })
      .failureHandler(Handlers::failureHandler);

    router.route(Constants.addStreamRoute + "*").handler(BodyHandler.create());
    router.route(Constants.addStreamRoute)
      .handler(routingContext -> {
      Stream stream = Stream.fromJson(routingContext.getBodyAsJson());

        vertx
          .eventBus()
          .request(
            STREAM_SERVICE_ADDRESS,
            Stream.asJson(stream),
            action("save"));

    routingContext.response()
        .setStatusCode(201)
        .putHeader("content-type", "application/json; charset=utf-8")
        .end(Stream.asJson(stream).toString());
      })
      .failureHandler(Handlers::failureHandler);

    router.route(Constants.addObservationRoute + "*").handler(BodyHandler.create());
    router.route(Constants.addObservationRoute)
      .handler(routingContext -> {

      })
      .failureHandler(Handlers::failureHandler);

    return router;
  }

  @Override
  public void start(Promise promise) throws Exception {
    val router = createRouter();

    ConfigRetriever retriever = ConfigRetriever.create(vertx);
    retriever.getConfig(
      config -> {
        if (config.failed()) {
          promise.fail(config.cause());
        } else {
          vertx
            .createHttpServer()
            .requestHandler(router)
            .listen(
              config().getInteger("HTTP_PORT", 8080),
              result -> {
                if (result.succeeded()) {
                  promise.complete();
                } else {
                  promise.fail(result.cause());
                }
              }
            );
        }
      }
    );
  }

  private static DeliveryOptions action(String actionString) {
    return new DeliveryOptions().addHeader("action", actionString);
  }
}
