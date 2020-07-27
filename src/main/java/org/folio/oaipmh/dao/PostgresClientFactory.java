package org.folio.oaipmh.dao;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.PreDestroy;

import org.folio.rest.persist.PostgresClient;
import org.jooq.Configuration;
import org.jooq.SQLDialect;
import org.jooq.impl.DefaultConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;

@Component
public class PostgresClientFactory {

  private static final Logger LOG = LoggerFactory.getLogger(PostgresClientFactory.class);

  public static final Configuration configuration = new DefaultConfiguration().set(SQLDialect.POSTGRES);

  private static final String HOST = "host";
  private static final String PORT = "port";
  private static final String DATABASE = "database";
  private static final String PASSWORD = "password";
  private static final String USERNAME = "username";

  private static final String DEFAULT_SCHEMA_PROPERTY = "search_path";

  private static final int POOL_SIZE = 5;

  private static final Map<String, PgPool> POOL_CACHE = new HashMap<>();

  private Vertx vertx;

  @Autowired
  public PostgresClientFactory(Vertx vertx) {
    LOG.debug("PostgresClientFactory constructor start");
    this.vertx = vertx;
    LOG.debug("PostgresClientFactory constructor finish");
  }

  @PreDestroy
  public void preDestory() {
    closeAll();
  }

  /**
   * Get {@link ReactiveClassicGenericQueryExecutor}
   *
   * @param tenantId tenant id
   * @return reactive query executor
   */
  public ReactiveClassicGenericQueryExecutor getQueryExecutor(String tenantId) {
    return new ReactiveClassicGenericQueryExecutor(configuration, getCachedPool(this.vertx, tenantId));
  }

  /**
   * Get {@link ReactiveClassicGenericQueryExecutor}
   *
   * @param vertx    current Vertx
   * @param tenantId tenant id
   * @return reactive query executor
   */
  public static ReactiveClassicGenericQueryExecutor getQueryExecutor(Vertx vertx, String tenantId) {
    return new ReactiveClassicGenericQueryExecutor(configuration, getCachedPool(vertx, tenantId));
  }

  public static void closeAll() {
    POOL_CACHE.values().forEach(PostgresClientFactory::close);
    POOL_CACHE.clear();
  }

  private static PgPool getCachedPool(Vertx vertx, String tenantId) {
    // assumes a single thread Vert.x model so no synchronized needed
    if (POOL_CACHE.containsKey(tenantId)) {
      LOG.debug("Using existing database connection pool for tenant {}", tenantId);
      return POOL_CACHE.get(tenantId);
    }
    LOG.info("Creating new database connection pool for tenant {}", tenantId);
    PgConnectOptions connectOptions = getConnectOptions(vertx, tenantId);
    PoolOptions poolOptions = new PoolOptions().setMaxSize(POOL_SIZE);
    PgPool client = PgPool.pool(vertx, connectOptions, poolOptions);
    POOL_CACHE.put(tenantId, client);
    return client;
  }

  // NOTE: This should be able to get database configuration without PostgresClient.
  // Additionally, with knowledge of tenant at this time, we are not confined to
  // schema isolation and can provide database isolation.
  private static PgConnectOptions getConnectOptions(Vertx vertx, String tenantId) {
    PostgresClient postgresClient = PostgresClient.getInstance(vertx, tenantId);
    JsonObject postgreSQLClientConfig = postgresClient.getConnectionConfig();
    postgresClient.closeClient(closed -> {
      if (closed.failed()) {
        LOG.error("Unable to close PostgresClient", closed.cause());
      }
    });
    return new PgConnectOptions()
      .setHost(postgreSQLClientConfig.getString(HOST))
      .setPort(postgreSQLClientConfig.getInteger(PORT))
      .setDatabase(postgreSQLClientConfig.getString(DATABASE))
      .setUser(postgreSQLClientConfig.getString(USERNAME))
      .setPassword(postgreSQLClientConfig.getString(PASSWORD))
      // using RMB convention driven tenant to schema name
      .addProperty(DEFAULT_SCHEMA_PROPERTY, PostgresClient.convertToPsqlStandard(tenantId));
  }

  private static void close(PgPool client) {
    client.close();
  }

}
