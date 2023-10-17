package org.traffic.traffic_producer.observations;

import io.vertx.core.Future;

public interface ObservationRepository {

  Future<Observation> publish(Observation observation);
}
