package org.folio.oaipmh.querybuilder;

import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class ViewTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(ViewTest.class);

  private static final Path EXPECTED_ALL_NON_DELETED_INSTANCE_IDS = Path.of("src/test/resources/views/expected_all_non_deleted_instance_ids.csv");
  private static final Path EXPECTED_ALL_DELETED_INSTANCE_IDS = Path.of("src/test/resources/views/expected_all_deleted_instance_ids.csv");
  private static final Path EXPECTED_FOLIO_DELETED_INSTANCE_IDS = Path.of("src/test/resources/views/expected_folio_deleted_instance_ids.csv");
  private static final Path EXPECTED_FOLIO_NON_DELETED_INSTANCE_IDS = Path.of("src/test/resources/views/expected_folio_non_deleted_instance_ids.csv");
  private static final Path EXPECTED_MARC_DELETED_INSTANCE_IDS = Path.of("src/test/resources/views/expected_marc_deleted_instance_ids.csv");
  private static final Path EXPECTED_MARC_NON_DELETED_INSTANCE_IDS = Path.of("src/test/resources/views/expected_marc_non_deleted_instance_ids.csv");

  private static final Network network = Network.newNetwork();

  @Container
  private static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:12-alpine")
    .withNetwork(network)
    .withNetworkAliases("postgres")
    .withExposedPorts(5432)
    .withUsername("username")
    .withPassword("password")
    .withDatabaseName("postgres")
    .withInitScript("sql/init_database_with_data.sql");

  @SneakyThrows
  @Test
  void shouldReturnAllNonDeletedInstances() {
    var query = QueryBuilder.build("oaitest", null, null, null, null,
      false, false, 200);
    LOGGER.debug("\n" + query);
    var actualResponse = doQuery(query, "instance_id");
    assertEquals(Files.readString(EXPECTED_ALL_NON_DELETED_INSTANCE_IDS).trim(), actualResponse.trim());
  }

  @SneakyThrows
  @Test
  void shouldReturnAllDeletedInstances() {
    var query = QueryBuilder.build("oaitest", null, null, null, null,
      false, true, 200);
    LOGGER.debug("\n" + query);
    var actualResponse = doQuery(query, "instance_id");
    assertEquals(Files.readString(EXPECTED_ALL_DELETED_INSTANCE_IDS).trim(), actualResponse.trim());
  }

  @SneakyThrows
  @Test
  void shouldReturnFolioNonDeletedInstances() {
    var query = QueryBuilder.build("oaitest", null, null, null, RecordsSource.FOLIO,
      false, false, 200);
    LOGGER.debug("\n" + query);
    var actualResponse = doQuery(query, "instance_id");
    assertEquals(Files.readString(EXPECTED_FOLIO_NON_DELETED_INSTANCE_IDS).trim(), actualResponse.trim());
  }

  @SneakyThrows
  @Test
  void shouldReturnFolioDeletedInstances() {
    var query = QueryBuilder.build("oaitest", null, null, null, RecordsSource.FOLIO,
      false, true, 200);
    LOGGER.debug("\n" + query);
    var actualResponse = doQuery(query, "instance_id");
    assertEquals(Files.readString(EXPECTED_FOLIO_DELETED_INSTANCE_IDS).trim(), actualResponse.trim());
  }

  @SneakyThrows
  @Test
  void shouldReturnMarcNonDeletedInstances() {
    var query = QueryBuilder.build("oaitest", null, null, null, RecordsSource.MARC,
      false, false, 200);
    LOGGER.debug("\n" + query);
    var actualResponse = doQuery(query, "instance_id");
    assertEquals(Files.readString(EXPECTED_MARC_NON_DELETED_INSTANCE_IDS).trim(), actualResponse.trim());
  }

  @SneakyThrows
  @Test
  void shouldReturnMarcDeletedInstances() {
    var query = QueryBuilder.build("oaitest", null, null, null, RecordsSource.MARC,
      false, true, 200);
    LOGGER.debug("\n" + query);
    var actualResponse = doQuery(query, "instance_id");
    assertEquals(Files.readString(EXPECTED_MARC_DELETED_INSTANCE_IDS).trim(), actualResponse.trim());
  }

  private String doQuery(String query, String... columns) throws SQLException {
    try (Connection connection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(),
      postgres.getPassword()); PreparedStatement preparedStatement = connection.prepareStatement(query)) {
      return getSqlResponse(preparedStatement.executeQuery(), columns);
    }
  }

  private String getSqlResponse(ResultSet resultSet, String... columns) throws SQLException {
    String rows = "";
    while (resultSet.next()) {
      String row = "";
      for (String col: columns) {
        var elem = resultSet.getObject(col);
        row += elem + ",";
      }
      row = row.substring(0, row.length() - 1);
      rows += row + "\n";
    }
    return rows;
  }

  @BeforeAll
  static void beforeAll() {
    postgres.start();
  }

  @AfterAll
  static void afterAll() {
    postgres.stop();
  }
}
