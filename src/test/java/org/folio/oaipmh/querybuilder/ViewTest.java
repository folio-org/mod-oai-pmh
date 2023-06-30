package org.folio.oaipmh.querybuilder;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

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
    .withInitScript("views/create_diku_mod_inventory_storage.sql");

  @Test
  void test() {

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
