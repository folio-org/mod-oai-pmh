package org.folio.rest.impl;

import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.OKAPI_TOKEN;
import static org.folio.oaipmh.Constants.OKAPI_URL;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.folio.oaipmh.service.ConfigurationCrudService;
import org.folio.rest.jaxrs.model.Configuration;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@ExtendWith(VertxExtension.class)
class OaiPmhConfigurationsImplTest {

  @Mock
  private ConfigurationCrudService configurationCrudService;

  @InjectMocks
  private OaiPmhConfigurationsImpl configurationsImpl;

  private final Map<String, String> okapiHeaders = new HashMap<>();

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    okapiHeaders.put(OKAPI_TENANT, "test_tenant");
    okapiHeaders.put(OKAPI_TOKEN, "test_token");
    okapiHeaders.put(OKAPI_URL, "http://localhost:9130");
  }

  @Test
  void testGetConfigurationsSuccess(Vertx vertx, VertxTestContext testContext) {
    List<Configuration> mockConfigurations = createMockConfigurations();
    when(configurationCrudService.getConfigurations(anyString(), anyString(), any(), anyString(), any(Integer.class), any(Integer.class)))
        .thenReturn(Future.succeededFuture(mockConfigurations));

    Context context = vertx.getOrCreateContext();
    configurationsImpl.getOaiPmhConfigurations(null, false, null, "auto", 0, 10, okapiHeaders,
        result -> {
          testContext.verify(() -> {
            assertThat(result.succeeded(), is(true));
            assertNotNull(result.result());
          });
          testContext.completeNow();
        }, context);
  }

  @Test
  void testGetConfigurationByIdSuccess(Vertx vertx, VertxTestContext testContext) {
    Configuration mockConfiguration = createMockConfiguration();
    when(configurationCrudService.getConfigurationById(anyString(), anyString()))
        .thenReturn(Future.succeededFuture(mockConfiguration));

    Context context = vertx.getOrCreateContext();
    configurationsImpl.getOaiPmhConfigurationsByConfigurationId("test-id", okapiHeaders,
        result -> {
          testContext.verify(() -> {
            assertThat(result.succeeded(), is(true));
            assertNotNull(result.result());
          });
          testContext.completeNow();
        }, context);
  }

  @Test
  void testGetConfigurationByIdNotFound(Vertx vertx, VertxTestContext testContext) {
    when(configurationCrudService.getConfigurationById(anyString(), anyString()))
        .thenReturn(Future.succeededFuture(null));

    Context context = vertx.getOrCreateContext();
    configurationsImpl.getOaiPmhConfigurationsByConfigurationId("non-existent-id", okapiHeaders,
        result -> {
          testContext.verify(() -> {
            assertThat(result.succeeded(), is(true));
            assertThat(result.result().getStatus(), is(404));
          });
          testContext.completeNow();
        }, context);
  }

  @Test
  void testCreateConfigurationSuccess(Vertx vertx, VertxTestContext testContext) {
    Configuration inputConfiguration = createMockConfiguration();
    Configuration createdConfiguration = createMockConfiguration();
    when(configurationCrudService.createConfiguration(any(Configuration.class), anyString()))
        .thenReturn(Future.succeededFuture(createdConfiguration));

    Context context = vertx.getOrCreateContext();
    configurationsImpl.postOaiPmhConfigurations(inputConfiguration, okapiHeaders,
        result -> {
          testContext.verify(() -> {
            assertThat(result.succeeded(), is(true));
            assertThat(result.result().getStatus(), is(201));
          });
          testContext.completeNow();
        }, context);
  }

  @Test
  void testUpdateConfigurationSuccess(Vertx vertx, VertxTestContext testContext) {
    Configuration inputConfiguration = createMockConfiguration();
    Configuration updatedConfiguration = createMockConfiguration();
    when(configurationCrudService.updateConfiguration(any(Configuration.class), anyString()))
        .thenReturn(Future.succeededFuture(updatedConfiguration));

    Context context = vertx.getOrCreateContext();
    configurationsImpl.putOaiPmhConfigurationsByConfigurationId("test-id", inputConfiguration, okapiHeaders,
        result -> {
          testContext.verify(() -> {
            assertThat(result.succeeded(), is(true));
            assertThat(result.result().getStatus(), is(204));
          });
          testContext.completeNow();
        }, context);
  }

  @Test
  void testDeleteConfigurationSuccess(Vertx vertx, VertxTestContext testContext) {
    when(configurationCrudService.deleteConfiguration(anyString(), anyString()))
        .thenReturn(Future.succeededFuture(true));

    Context context = vertx.getOrCreateContext();
    configurationsImpl.deleteOaiPmhConfigurationsByConfigurationId("test-id", okapiHeaders,
        result -> {
          testContext.verify(() -> {
            assertThat(result.succeeded(), is(true));
            assertThat(result.result().getStatus(), is(204));
          });
          testContext.completeNow();
        }, context);
  }

  private Configuration createMockConfiguration() {
    Value value = new Value();
    value.setAdditionalProperty("repository.name", "Test Repository");
    value.setAdditionalProperty("repository.baseURL", "http://test.org/oai");

    Metadata metadata = new Metadata()
        .withCreatedDate(new Date())
        .withCreatedByUserId(UUID.randomUUID().toString());

    return new Configuration()
        .withId(UUID.randomUUID().toString())
        .withConfigName("general")
        .withDescription("Test configuration")
        .withEnabled(true)
        .withValue(value)
        .withMetadata(metadata);
  }

  private List<Configuration> createMockConfigurations() {
    List<Configuration> configurations = new ArrayList<>();
    configurations.add(createMockConfiguration());
    return configurations;
  }
}
