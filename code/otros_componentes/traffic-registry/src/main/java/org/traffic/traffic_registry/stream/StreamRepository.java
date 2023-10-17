package org.traffic.traffic_registry.stream;

import io.vertx.core.Future;

import static java.lang.String.format;

public interface StreamRepository {

  Future<String> save(Stream stream);

  Future<String> findById(String id);

  Future<String> findAll();

  static String toLocalName(String id) {
    return format("/streams/%s", id);
  }
}
