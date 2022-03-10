package org.folio.rest.impl;

import static io.restassured.RestAssured.given;
import static io.restassured.RestAssured.when;
import static org.folio.rest.impl.OkapiMockServer.OAI_TEST_TENANT;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import java.nio.file.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockserver.client.MockServerClient;
import org.mockserver.model.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MockServerContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import io.restassured.RestAssured;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;

@Testcontainers
class ModTenantAPIIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(ModTenantAPIIT.class);

  private static final Network network = Network.newNetwork();

  @Container
  private static final GenericContainer<?> module =
    new GenericContainer<>(new ImageFromDockerfile("mod-data-export").withFileFromPath(".", Path.of(".")))
      .withNetwork(network)
      .withNetworkAliases("module")
      .withExposedPorts(8081)
      .withEnv("DB_HOST", "postgres")
      .withEnv("DB_PORT", "5432")
      .withEnv("DB_USERNAME", "username")
      .withEnv("DB_PASSWORD", "password")
      .withEnv("DB_DATABASE", "postgres");

  @Container
  private static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:12-alpine")
    .withNetwork(network)
    .withNetworkAliases("postgres")
    .withExposedPorts(5432)
    .withUsername("username")
    .withPassword("password")
    .withDatabaseName("postgres");

  @Container
  private static final MockServerContainer okapi =
    new MockServerContainer(DockerImageName.parse("mockserver/mockserver:mockserver-5.11.2"))
      .withNetwork(network)
      .withNetworkAliases("okapi")
      .withExposedPorts(1080);

  @BeforeAll
  static void beforeAll() {
    module.followOutput(new Slf4jLogConsumer(LOGGER).withSeparateOutputStreams());
    RestAssured.baseURI = "http://" + module.getHost() + ":" + module.getFirstMappedPort();

    var mockServerClient = new MockServerClient(okapi.getHost(), okapi.getServerPort());
    mockServerClient.when(request().withMethod("POST"))
      .respond(response().withStatusCode(201));
    mockServerClient.when(request().withMethod("GET"))
      .respond(response().withStatusCode(200).withBody("{\"configs\":[]}", MediaType.JSON_UTF_8));

    RestAssured.requestSpecification = new RequestSpecBuilder()
      .addHeader("x-okapi-tenant", OAI_TEST_TENANT)
      .addHeader("x-okapi-url", "http://okapi:1080")
      .setContentType(ContentType.JSON)
      .build();
  }

  @Test
  void healthTest() {
    when().
      get("/admin/health").
    then().
      statusCode(200);
  }

  @Test
  void tenantApiShouldReturn200AndDatabaseShouldBePopulated() {
    given().
      body("{ \"module_to\": \"99.99.99\" }").
    when().
      post("/_/tenant").
    then().
      statusCode(200);

    when().
      post("/oai-pmh/clean-up-instances").
    then().
      statusCode(204);
  }

}
