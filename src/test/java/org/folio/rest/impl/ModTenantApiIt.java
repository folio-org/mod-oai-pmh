package org.folio.rest.impl;

import static io.restassured.RestAssured.given;
import static io.restassured.RestAssured.when;
import static org.folio.rest.impl.OkapiMockServer.OAI_TEST_TENANT;

import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.NginxContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class ModTenantApiIt {

  private static final Logger LOGGER = LoggerFactory.getLogger(ModTenantApiIt.class);

  private static final Network network = Network.newNetwork();

  @Container
  private static final GenericContainer<?> module =
      new GenericContainer<>(new ImageFromDockerfile("mod-data-export")
          .withFileFromPath(".", Path.of(".")))
          .withNetwork(network)
          .withNetworkAliases("module")
          .withExposedPorts(8081)
          .withEnv("DB_HOST", "postgres")
          .withEnv("DB_PORT", "5432")
          .withEnv("DB_USERNAME", "username")
          .withEnv("DB_PASSWORD", "password")
          .withEnv("DB_DATABASE", "postgres")
          .withLogConsumer(new Slf4jLogConsumer(LOGGER, true).withPrefix("module"));

  @Container
  private static final PostgreSQLContainer<?> postgres =
      new PostgreSQLContainer<>("postgres:16-alpine")
        .withNetwork(network)
        .withNetworkAliases("postgres")
        .withExposedPorts(5432)
        .withUsername("username")
        .withPassword("password")
        .withDatabaseName("postgres")
        .withInitScript("sql/init_database.sql");

  @Container
  private static final NginxContainer<?> okapi = new NginxContainer<>("nginx:stable-alpine-slim")
      .withNetwork(network)
      .withNetworkAliases("okapi")
      .withExposedPorts(8080)
      .withLogConsumer(new Slf4jLogConsumer(LOGGER, true).withPrefix("okapi"))
      .withCopyToContainer(Transferable.of("""
            server {
              listen unix:/tmp/backend.sock;
              default_type application/json;
              location /GET/  { return 200 '{ "configs": []}'; }
              location /POST/ { return 201; }
            }
            server {
              listen 8080;
              location / { proxy_pass http://unix:/tmp/backend.sock:/$request_method$request_uri; }
            }
            """), "/etc/nginx/conf.d/default.conf");

  @BeforeAll
  static void beforeAll() {
    RestAssured.baseURI = "http://" + module.getHost() + ":" + module.getFirstMappedPort();
    RestAssured.requestSpecification = new RequestSpecBuilder()
        .addHeader("x-okapi-tenant", OAI_TEST_TENANT)
        .addHeader("x-okapi-url", "http://okapi:8080")
        .setContentType(ContentType.JSON)
        .build();
  }

  @Test
  void healthTest() {
    when().get("/admin/health")
      .then().statusCode(200);
  }

  @Test
  void tenantApiShouldReturn200AndDatabaseShouldBePopulated() {
    given().body("{ \"module_to\": \"99.99.99\" }")
        .when().post("/_/tenant")
        .then().statusCode(200);

    when().post("/oai-pmh/clean-up-instances").then().statusCode(204);
  }

}
