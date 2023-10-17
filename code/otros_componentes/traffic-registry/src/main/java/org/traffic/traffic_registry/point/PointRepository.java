package org.traffic.traffic_registry.point;

import io.vertx.core.Future;

import static java.lang.String.format;

public interface PointRepository {

  Future<String> save(Point point);

  Future<String> findById(String id);

  static String toLocalName(String id) {
    return format("/points/%s", id);
  }
}
