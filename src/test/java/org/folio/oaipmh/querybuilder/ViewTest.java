package org.folio.oaipmh.querybuilder;

import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
public class ViewTest {

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
  void test() {
    try (Connection connection = DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
         PreparedStatement preparedStatement = connection.prepareStatement("SELECT COUNT(*) FROM oaitest_mod_inventory_storage.instance")) {
      try (ResultSet resultSet = preparedStatement.executeQuery()) {
        if (resultSet.next()) {
          assertEquals(102, resultSet.getInt(1));
        }
      }
    }

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
