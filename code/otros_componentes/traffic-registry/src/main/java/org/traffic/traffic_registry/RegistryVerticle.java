package org.traffic.traffic_registry;

import com.stardog.stark.Values;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.servicediscovery.Record;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.types.HttpEndpoint;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.traffic.traffic_registry.sensor.SensorRepository;
import org.traffic.traffic_registry.stream.StreamRepository;

import static org.traffic.traffic_registry.sensor.SensorService.SENSOR_SERVICE_ADDRESS;
import static org.traffic.traffic_registry.stream.StreamService.STREAM_SERVICE_ADDRESS;

@Slf4j
@NoArgsConstructor
public final class RegistryVerticle extends AbstractVerticle {

  private HttpServer server;

  private ServiceDiscovery serviceDiscovery;

  private Record registryRecord;

  private String apiNamespace;

  @Override
  public void start(Promise<Void> startPromise) {

    val port = config().getInteger("port");
    val host = config().getString("host");
    apiNamespace = config().getString("api-namespace");
    val router = buildRouter();

    vertx
        .createHttpServer()
        .requestHandler(router)
        .listen(port)
        .compose(
            server -> {
              this.server = server;
              this.serviceDiscovery = ServiceDiscovery.create(vertx);
              val record = HttpEndpoint.createRecord("traffic-registry", host, port, "/api");
              return this.serviceDiscovery.publish(record);
            })
        .onSuccess(
            record -> {
              this.registryRecord = record;
              startPromise.complete();
            })
        .onFailure(
            cause -> {
              log.debug("Registry deployment failed");
              startPromise.fail(cause);
            });
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    serviceDiscovery
        .unpublish(registryRecord.getRegistration())
        .compose(__ -> server.close())
        .onComplete(
            result -> {
              serviceDiscovery.close();
              if (result.succeeded()) stopPromise.complete();
              else stopPromise.fail(result.cause());
            });
  }

  private Router buildRouter() {
    val router = Router.router(vertx);
    val bodyHandler = BodyHandler.create();

    val v1Router = Router.router(vertx);
    v1Router
        .post("/sensors")
        .consumes("application/json")
        .produces("text/turtle")
        .handler(bodyHandler)
        .handler(this::saveSensor);

    v1Router.get("/sensors").produces("text/turtle").handler(this::getSensors);
    v1Router.get("/sensors/:id").produces("text/turtle").handler(this::getSensor);

    v1Router
        .post("/streams")
        .consumes("application/json")
        .produces("text/turtle")
        .handler(bodyHandler)
        .handler(this::saveStream);

    v1Router.get("/streams/:id").produces("text/turtle").handler(this::getStream);
    v1Router.get("/streams/").produces("text/turtle").handler(this::getStreams);

    router.mountSubRouter("/api/v1", v1Router);
    return router;
  }

  private void getStreams(RoutingContext routingContext) {
    val action = new DeliveryOptions().addHeader("action", "findAll");
    vertx
        .eventBus()
        .<JsonObject>request(STREAM_SERVICE_ADDRESS, new JsonObject(), action)
        .onSuccess(
            reply -> {
              val rdf = reply.body().getString("result");
              val response = routingContext.response();
              response.setStatusCode(200);
              response.headers().add("Content-Type", "text/turtle");
              response.end(rdf);
            })
        .onFailure(
            throwable -> {
              val response = routingContext.response();
              val cause = ((ReplyException) throwable);
              response.setStatusCode(cause.failureCode());
              response.end();
            });
  }

  private void getStream(RoutingContext routingContext) {
    val streamId = routingContext.request().getParam("id");
    val action = new DeliveryOptions().addHeader("action", "find");
    vertx
        .eventBus()
        .<JsonObject>request(STREAM_SERVICE_ADDRESS, new JsonObject().put("id", streamId), action)
        .onSuccess(
            reply -> {
              val rdf = reply.body().getString("result");
              val response = routingContext.response();
              response.setStatusCode(200);
              response.headers().add("Content-Type", "text/turtle");
              response.end(rdf);
            })
        .onFailure(
            throwable -> {
              val response = routingContext.response();
              val cause = ((ReplyException) throwable);
              response.setStatusCode(cause.failureCode());
              response.end();
            });
  }

  private void getSensor(RoutingContext routingContext) {
    val sensorId = routingContext.request().getParam("id");
    val action = new DeliveryOptions().addHeader("action", "find");
    vertx
        .eventBus()
        .<JsonObject>request(SENSOR_SERVICE_ADDRESS, new JsonObject().put("id", sensorId), action)
        .onSuccess(
            reply -> {
              val rdf = reply.body().getString("result");
              val response = routingContext.response();
              response.setStatusCode(200);
              response.headers().add("Content-Type", "text/turtle");
              response.end(rdf);
            })
        .onFailure(
            throwable -> {
              val response = routingContext.response();
              val cause = ((ReplyException) throwable);
              response.setStatusCode(cause.failureCode());
              response.end();
            });
  }

  private void getSensors(RoutingContext routingContext) {
    val action = new DeliveryOptions().addHeader("action", "findAll");
    vertx
        .eventBus()
        .<JsonObject>request(SENSOR_SERVICE_ADDRESS, new JsonObject(), action)
        .onSuccess(
            reply -> {
              val rdf = reply.body().getString("result");
              val response = routingContext.response();
              response.setStatusCode(200);
              response.headers().add("Content-Type", "text/turtle");
              response.end(rdf);
            })
        .onFailure(
            throwable -> {
              val response = routingContext.response();
              val cause = ((ReplyException) throwable);
              response.setStatusCode(cause.failureCode());
              response.end();
            });
  }

  private void saveSensor(RoutingContext routingContext) {
    val sensorJson = routingContext.getBodyAsJson();
    val action = new DeliveryOptions().addHeader("action", "save");
    vertx
        .eventBus()
        .<JsonObject>request(SENSOR_SERVICE_ADDRESS, sensorJson, action)
        .onSuccess(
            reply -> {
              val rdf = reply.body().getString("result");
              val response = routingContext.response();
              response.setStatusCode(201);
              response
                  .headers()
                  .add(
                      "Location",
                      Values.iri(
                              apiNamespace,
                              SensorRepository.toLocalName(sensorJson.getString("id")))
                          .toString())
                  .add("Content-Type", "text/turtle");
              response.end(rdf);
            })
        .onFailure(
            throwable -> {
              val cause = ((ReplyException) throwable);
              routingContext.response().setStatusCode(cause.failureCode()).end(cause.getMessage());
            });
  }

  private void saveStream(RoutingContext routingContext) {
    val streamJson = routingContext.getBodyAsJson();
    val action = new DeliveryOptions().addHeader("action", "save");
    vertx
        .eventBus()
        .<JsonObject>request(STREAM_SERVICE_ADDRESS, streamJson, action)
        .onSuccess(
            reply -> {
              val rdf = reply.body().getString("result");
              val response = routingContext.response();
              response.setStatusCode(201);
              response
                  .headers()
                  .add(
                      "Location",
                      Values.iri(
                              apiNamespace,
                              StreamRepository.toLocalName(streamJson.getString("id")))
                          .toString())
                  .add("Content-Type", "text/turtle");
              response.end(rdf);
            })
        .onFailure(
            throwable -> {
              val cause = ((ReplyException) throwable);
              routingContext.response().setStatusCode(cause.failureCode()).end(cause.getMessage());
            });
  }
}
