package org.folio.oaipmh.dao;

import static java.util.Objects.nonNull;

import io.github.jklingsporn.vertx.jooq.classic.reactivepg.ReactiveClassicGenericQueryExecutor;
import io.vertx.core.Context;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
  private static final String HOST_READER = "host_reader";
  private static final String PORT = "port";
  private static final String PORT_READER = "port_reader";
  private static final String DATABASE = "database";
  private static final String PASSWORD = "password";
  private static final String USERNAME = "username";

  private static final String DEFAULT_SCHEMA_PROPERTY = "search_path";

  private static final int POOL_SIZE = 20;

  private static boolean readWriteSplitEnabled = true;

  /**
   * Such field is temporary solution which is used to allow resetting the pool in tests.
   * In future the {@link org.folio.oaipmh.processors.MarcWithHoldingsRequestHelper#getNextBatch(String, Request, int, Promise, Context, Long)}
   * should be canceled when response with failure already responded.
   */
  private static boolean shouldResetPool = false;

  private static final Map<String, PgPool> POOL_CACHE = new HashMap<>();
  private static final Map<String, PgPool> POOL_CACHE_READER = new HashMap<>();

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
    return new ReactiveClassicGenericQueryExecutor(configuration, getCachedPool(this.vertx, tenantId, false));
  }

  public ReactiveClassicGenericQueryExecutor getQueryExecutorReader(String tenantId) {
    return new ReactiveClassicGenericQueryExecutor(configuration, getCachedPool(this.vertx, tenantId, true));
  }

  public static void closeAll() {
    POOL_CACHE.values().forEach(PostgresClientFactory::close);
    POOL_CACHE.clear();
    POOL_CACHE_READER.values().forEach(PostgresClientFactory::close);
    POOL_CACHE_READER.clear();
  }

  private static PgPool getCachedPool(Vertx vertx, String tenantId, boolean isReader) {
    // assumes a single thread Vert.x model so no synchronized needed
    var pool = isReader && readWriteSplitEnabled ? POOL_CACHE_READER : POOL_CACHE;
    if (pool.containsKey(tenantId) && !shouldResetPool) {
      logger.debug("Using existing database connection pool for tenant {}.", tenantId);
      return pool.get(tenantId);
    }
    if (shouldResetPool) {
      pool.remove(tenantId);
      shouldResetPool = false;
    }
    return createCachedPool(vertx, tenantId, isReader);
  }

  private static PgPool createCachedPool(Vertx vertx, String tenantId, boolean isReader) {
    logger.info("Creating new database connection pool for tenant {}.", tenantId);
    PgConnectOptions connectOptions = getConnectOptions(vertx, tenantId, isReader);
    var pool = isReader && readWriteSplitEnabled ? POOL_CACHE_READER : POOL_CACHE;
    PoolOptions poolOptions = new PoolOptions().setMaxSize(POOL_SIZE);
    PgPool client = PgPool.pool(vertx, connectOptions, poolOptions);
    pool.put(tenantId, client);
    return client;
  }

  // NOTE: This should be able to get database configuration without PostgresClient.
  // Additionally, with knowledge of tenant at this time, we are not confined to
  // schema isolation and can provide database isolation.
  private static PgConnectOptions getConnectOptions(Vertx vertx, String tenantId, boolean isReader) {
    PostgresClient postgresClient = PostgresClient.getInstance(vertx, tenantId);
    JsonObject postgreSQLClientConfig = postgresClient.getConnectionConfig();
    postgresClient.closeClient();
    readWriteSplitEnabled = nonNull(postgreSQLClientConfig.getString(HOST_READER)) && nonNull(postgreSQLClientConfig.getInteger(PORT_READER));
    var host = isReader && readWriteSplitEnabled ?
      postgreSQLClientConfig.getString(HOST_READER) :
      postgreSQLClientConfig.getString(HOST);
    var port = isReader && readWriteSplitEnabled ?
      postgreSQLClientConfig.getInteger(PORT_READER) :
      postgreSQLClientConfig.getInteger(PORT);
    logger.info("Is reader: {}, connection parameters: {}:{}", readWriteSplitEnabled, host, port);
    return new PgConnectOptions()
      .setHost(host)
      .setPort(port)
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
