package org.traffic.traffic_registry.sensor;

import io.vertx.core.Future;

import static java.lang.String.format;

public interface SensorRepository {

  Future<String> save(Sensor sensor);

  Future<String> findById(String id);

  Future<String> findAll();

  static String toLocalName(String id) {
    return format("/sensors/%s", id);
  }
}
