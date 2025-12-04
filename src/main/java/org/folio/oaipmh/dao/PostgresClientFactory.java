package org.folio.oaipmh.dao;

import static java.util.Objects.nonNull;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.SqlConnectOptions;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.PreDestroy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.processors.GetListRecordsRequestHelper;
import org.folio.rest.persist.PostgresClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class PostgresClientFactory {

  private static final Logger logger = LogManager.getLogger(PostgresClientFactory.class);

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
   * In future the {@link GetListRecordsRequestHelper#getNextBatch(String, Request, int,
   * Promise, Context, Long)} should be canceled when response with failure already responded.
   */
  private static boolean shouldResetPool = false;

  private static final Map<String, Pool> POOL_CACHE = new HashMap<>();
  private static final Map<String, Pool> POOL_CACHE_READER = new HashMap<>();

  private final Vertx vertx;

  @Autowired
  public PostgresClientFactory(Vertx vertx) {
    this.vertx = vertx;
  }

  @PreDestroy
  public void preDestroy() {
    closeAll();
  }

  public Pool getPoolWriter(String tenantId) {
    return getCachedPool(vertx, tenantId, false);
  }

  public Pool getPoolReader(String tenantId) {
    return getCachedPool(vertx, tenantId, true);
  }

  public SqlClient getReaderClient(String tenantId) {
    SqlConnectOptions connectOptions = getConnectOptions(vertx, tenantId, true);
    PoolOptions poolOptions = new PoolOptions().setMaxSize(POOL_SIZE);
    return Pool.pool(vertx, connectOptions, poolOptions);
  }

  public static void closeAll() {
    POOL_CACHE.values().forEach(PostgresClientFactory::close);
    POOL_CACHE.clear();
    POOL_CACHE_READER.values().forEach(PostgresClientFactory::close);
    POOL_CACHE_READER.clear();
  }

  private static Pool getCachedPool(Vertx vertx, String tenantId, boolean isReader) {
    Map<String, Pool> cache = isReader && readWriteSplitEnabled ? POOL_CACHE_READER : POOL_CACHE;

    if (cache.containsKey(tenantId) && !shouldResetPool) {
      logger.debug("Using existing database connection pool for tenant {}.", tenantId);
      return cache.get(tenantId);
    }

    if (shouldResetPool) {
      cache.remove(tenantId);
      shouldResetPool = false;
    }

    return createCachedPool(vertx, tenantId, isReader);
  }

  private static Pool createCachedPool(Vertx vertx, String tenantId, boolean isReader) {
    logger.info("Creating new database connection pool for tenant {} (reader: {}).",
        tenantId, isReader);

    SqlConnectOptions connectOptions = getConnectOptions(vertx, tenantId, isReader);
    Map<String, Pool> cache = isReader && readWriteSplitEnabled ? POOL_CACHE_READER : POOL_CACHE;

    PoolOptions poolOptions = new PoolOptions().setMaxSize(POOL_SIZE);
    Pool client = Pool.pool(vertx, connectOptions, poolOptions);
    cache.put(tenantId, client);
    return client;
  }

  // NOTE: This should be able to get database configuration without PostgresClient.
  // Additionally, with knowledge of tenant at this time, we are not confined to
  // schema isolation and can provide database isolation.
  private static SqlConnectOptions getConnectOptions(Vertx vertx, String tenantId,
      boolean isReader) {
    PostgresClient postgresClient = PostgresClient.getInstance(vertx, tenantId);
    JsonObject postgreSqlClientConfig = postgresClient.getConnectionConfig();
    postgresClient.closeClient();

    readWriteSplitEnabled = nonNull(postgreSqlClientConfig.getString(HOST_READER))
        && nonNull(postgreSqlClientConfig.getInteger(PORT_READER));

    String host = isReader && readWriteSplitEnabled
        ? postgreSqlClientConfig.getString(HOST_READER)
        : postgreSqlClientConfig.getString(HOST);

    Integer port = isReader && readWriteSplitEnabled
        ? postgreSqlClientConfig.getInteger(PORT_READER)
        : postgreSqlClientConfig.getInteger(PORT);

    logger.info("Is reader: {}, connection parameters: {}:{}",
        isReader && readWriteSplitEnabled, host, port);

    return new SqlConnectOptions()
        .setHost(host)
        .setPort(port)
        .setDatabase(postgreSqlClientConfig.getString(DATABASE))
        .setUser(postgreSqlClientConfig.getString(USERNAME))
        .setPassword(postgreSqlClientConfig.getString(PASSWORD))
        .addProperty(DEFAULT_SCHEMA_PROPERTY, PostgresClient.convertToPsqlStandard(tenantId));
  }

  private static void close(Pool client) {
    client.close();
  }

  public static void setShouldResetPool(boolean shouldResetPool) {
    PostgresClientFactory.shouldResetPool = shouldResetPool;
  }
}