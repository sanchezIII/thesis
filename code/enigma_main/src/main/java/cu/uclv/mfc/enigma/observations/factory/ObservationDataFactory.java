package cu.uclv.mfc.enigma.observations.factory;

import cu.uclv.mfc.enigma.observations.data.ObservationData;
import io.vertx.core.json.JsonObject;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import lombok.AllArgsConstructor;
import lombok.val;

@AllArgsConstructor
public class ObservationDataFactory {

  private Class obsDataClass;

  public ObservationData createObservationData(JsonObject jsonData) {
    Class[] typeArgs = { JsonObject.class };

    try {
      val dummyObservationData = (ObservationData) obsDataClass
        .getDeclaredConstructor()
        .newInstance();

      Method fromJsonMethod = obsDataClass.getMethod(
        "getObservationDataFromJson",
        typeArgs
      );

      return (ObservationData) fromJsonMethod.invoke(
        dummyObservationData,
        jsonData
      );
    } catch (NoSuchMethodException e) {
      e.printStackTrace();
    } catch (InvocationTargetException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    }
    return null;
  }

  public ObservationData createDummyObservationData() {
    try {
      return (ObservationData) obsDataClass
        .getDeclaredConstructor()
        .newInstance();
    } catch (InstantiationException e) {
      System.out.println(
        "Reflect Error: InstantiationException at createDummyObservationData"
      );
    } catch (IllegalAccessException e) {
      System.out.println(
        "Reflect Error: IllegalAccessException at createDummyObservationData"
      );
    } catch (InvocationTargetException e) {
      System.out.println(
        "Reflect Error: InvocationTargetException at createDummyObservationData"
      );
    } catch (NoSuchMethodException e) {
      System.out.println(
        "Reflect Error: NoSuchMethodException at createDummyObservationData"
      );
    }
    return null;
  }
}
