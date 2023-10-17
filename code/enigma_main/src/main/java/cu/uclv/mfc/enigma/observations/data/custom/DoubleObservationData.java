package cu.uclv.mfc.enigma.observations.data.custom;

import cu.uclv.mfc.enigma.observations.data.ObservationData;
import io.vertx.core.json.JsonObject;

// JsonFormat
// {"value": 23.07}

public class DoubleObservationData implements ObservationData {

  private Double value;

  @Override
  public boolean equals(ObservationData other) {
    if (!(other.getValue() instanceof Double)) return false;
    return value.equals((Double) other.getValue());
  }

  @Override
  public Object getValue() {
    return value;
  }

  @Override
  public void setValue(Object value) {
    this.value = (Double) value;
  }

  @Override
  public JsonObject getJsonRepresentation() {
    return new JsonObject().put("value", value);
  }

  @Override
  public ObservationData getObservationDataFromJson(JsonObject json) {
    ObservationData obsData = new DoubleObservationData();
    obsData.setValue(json.getDouble("value"));
    return obsData;
  }
}
