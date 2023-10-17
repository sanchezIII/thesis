package cu.uclv.mfc.enigma.observations;

import cu.uclv.mfc.enigma.observations.data.Observation;
import cu.uclv.mfc.enigma.topics.TopicCollection;
import io.vertx.core.Future;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public abstract class ObservationRepository {

  protected TopicCollection topics;

  public abstract Future<Observation> publish(
    Observation observation,
    String topicId
  );
}
