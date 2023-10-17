package org.traffic.traffic_registry.common;

import com.complexible.stardog.api.ConnectionConfiguration;
import com.complexible.stardog.api.admin.AdminConnectionConfiguration;
import com.complexible.stardog.rdf4j.StardogRepository;
import io.vertx.core.json.JsonObject;
import lombok.val;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.util.Values;
import org.eclipse.rdf4j.repository.Repository;

public abstract class AbstractStardogRDFRepository {

  protected final Repository repository;

  protected final Namespace namespace;

  public AbstractStardogRDFRepository(JsonObject config) {

    val databaseName = config.getString("database-name");
    val databaseUrl = config.getString("database-url");
    val server = config.getString("server");
    val username = config.getString("username");
    val password = config.getString("password");

    try (val adminConnection =
        AdminConnectionConfiguration.toServer(server).credentials(username, password).connect()) {
      if (!adminConnection.list().contains(databaseName)) {
        adminConnection.newDatabase(databaseName).create();
      }
    }

    this.repository =
        new StardogRepository(
            ConnectionConfiguration.from(databaseUrl).credentials(username, password));

    if (!this.repository.isInitialized()) this.repository.initialize();

    val namespace = config.getString("namespace");
    val prefix = config.getString("prefix");

    this.namespace = Values.namespace(prefix, namespace);
  }

  public AbstractStardogRDFRepository(Repository repository, String prefix, String namespace) {
    this.namespace = Values.namespace(prefix, namespace);
    this.repository = repository;
  }
}
