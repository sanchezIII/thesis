package cu.uclv.mfc.enigma.util;

import io.vertx.core.eventbus.DeliveryOptions;

public class ObjectBuilder {

  public static DeliveryOptions action(String actionString) {
    return new DeliveryOptions().addHeader("action", actionString);
  }
}
