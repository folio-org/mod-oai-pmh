package org.folio.oaipmh.dao;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.vertx.core.Context;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;

import org.folio.oaipmh.Request;
import org.folio.rest.persist.PostgresClient;
import org.jooq.Configuration;
import org.jooq.SQLDialect;
import org.jooq.impl.DefaultConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.HashMap;
import java.util.Map;

@Component
public class PostgresClientFactory {

  private static final Logger logger = LogManager.getLogger(PostgresClientFactory.class);

  public static final Configuration configuration = new DefaultConfiguration().set(SQLDialect.POSTGRES);

  private static final String HOST = "host";
  private static final String PORT = "port";
  private static final String DATABASE = "database";
  private static final String PASSWORD = "password";
  private static final String USERNAME = "username";

  private static final String DEFAULT_SCHEMA_PROPERTY = "search_path";

  private static final int POOL_SIZE = 20;

  /**
   * Such field is temporary solution which is used to allow resetting the pool in tests.
   * In future the {@link org.folio.oaipmh.processors.MarcWithHoldingsRequestHelper#getNextBatch(String, Request, int, Promise, Context, Long)}
   * should be canceled when response with failure already responded.
   */
  private static boolean shouldResetPool = false;

  private static final Map<String, PgPool> POOL_CACHE = new HashMap<>();

  private Vertx vertx;

  @Autowired
  public PostgresClientFactory(Vertx vertx) {
    this.vertx = vertx;
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
    if (POOL_CACHE.containsKey(tenantId) && !shouldResetPool) {
      logger.debug("Using existing database connection pool for tenant {}.", tenantId);
      return POOL_CACHE.get(tenantId);
    }
    if (shouldResetPool) {
      POOL_CACHE.remove(tenantId);
      shouldResetPool = false;
    }
    logger.info("Creating new database connection pool for tenant {}.", tenantId);
    PgConnectOptions connectOptions = getConnectOptions(vertx, tenantId);
    PoolOptions poolOptions = new PoolOptions().setMaxSize(POOL_SIZE);
    PgPool client = PgPool.pool(vertx, connectOptions, poolOptions);
    POOL_CACHE.put(tenantId, client);
    return client;
  }

  public static PgPool getPool(Vertx vertx, String tenantId) {
    return getCachedPool(vertx, tenantId);
  }

  // NOTE: This should be able to get database configuration without PostgresClient.
  // Additionally, with knowledge of tenant at this time, we are not confined to
  // schema isolation and can provide database isolation.
  private static PgConnectOptions getConnectOptions(Vertx vertx, String tenantId) {
    PostgresClient postgresClient = PostgresClient.getInstance(vertx, tenantId);
    JsonObject postgreSQLClientConfig = postgresClient.getConnectionConfig();
    postgresClient.closeClient(closed -> {
      if (closed.failed()) {
        logger.error("Unable to close PostgresClient.", closed.cause());
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

  public static void setShouldResetPool(boolean shouldResetPool) {
    PostgresClientFactory.shouldResetPool = shouldResetPool;
  }
}
