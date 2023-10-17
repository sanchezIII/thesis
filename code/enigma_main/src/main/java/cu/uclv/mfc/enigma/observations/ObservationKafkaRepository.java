package cu.uclv.mfc.enigma.observations;

import cu.uclv.mfc.enigma.observations.data.Observation;
import cu.uclv.mfc.enigma.topics.TopicCollection;
import io.vertx.core.Future;

public class ObservationKafkaRepository extends ObservationRepository {

  public ObservationKafkaRepository(TopicCollection topics) {
    super(topics);
  }

  @Override
  public Future<Observation> publish(Observation observation, String topicId) {
    return null;
  }
}
