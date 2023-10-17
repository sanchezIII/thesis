package org.traffic.traffic_producer.streams;

import io.vertx.core.Future;

import java.util.List;

/**
 * Interfaz con la base de datos
 */
public interface StreamRepository {

  Future<Stream> save(Stream stream);

  Future<List<Stream>> saveAll(List<Stream> streamList);

  Future<Stream> find(String sensorId, String feature);

  Future<List<Stream>> findAll();

  Future<List<Stream>> findAll(List<String> sensorIds);

  Future<List<Stream>> deleteAllBy(List<String> sensorIds);
}
