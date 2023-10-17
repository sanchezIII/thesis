package org.traffic.traffic_producer.sensors;

import io.vertx.core.Future;

import java.util.List;

public interface SensorRepository {

  Future<List<Sensor>> saveAll(List<Sensor> sensor);

  Future<Sensor> save(Sensor sensor);
}
