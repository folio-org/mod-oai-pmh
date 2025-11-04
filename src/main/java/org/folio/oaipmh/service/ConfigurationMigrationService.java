package org.folio.oaipmh.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.log4j.Log4j2;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.spring.FolioExecutionContext;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;


@Log4j2
@Service
public class ConfigurationMigrationService {

  private static final String
      MOD_CONFIGURATION_QUERY = "/configurations/entries?query=module==OAIPMH";
  private static final Map<String, String> CONFIG_NAME_MAPPING = Map.of(
      "behavior", "behavioral",
      "behavioral", "behavioral",
      "general", "general",
      "technical", "technical"
  );

  private final JdbcTemplate jdbcTemplate;
  private final FolioExecutionContext context;
  private final RestTemplate restTemplate;
  private final ObjectMapper objectMapper;

  public ConfigurationMigrationService(JdbcTemplate jdbcTemplate,
                                       FolioExecutionContext context,
                                       RestTemplate restTemplate,
                                       ObjectMapper objectMapper) {
    this.jdbcTemplate = jdbcTemplate;
    this.context = context;
    this.restTemplate = restTemplate;
    this.objectMapper = objectMapper;
  }

  public void migrateConfigurationsFromModConfiguration() {
    log.info("Starting migration of configurations from "
        + "mod-configuration for tenant: {}", context.getTenantId());

    try {
      // Check if migration has already been done
      Integer migrationCount = jdbcTemplate.queryForObject(
          "SELECT COUNT(*) FROM " + context.getTenantId()
            + "_mod_oai_pmh.configuration_settings WHERE config_name = '__migration_completed__'",
          Integer.class
      );

      if (migrationCount != null && migrationCount > 0) {
        log.info("Configuration migration already completed, skipping");
        return;
      }

      // Fetch configurations from mod-configuration
      String okapiUrl = context.getOkapiUrl();
      if (okapiUrl == null || okapiUrl.isEmpty()) {
        log.warn("Okapi URL not available, skipping migration from mod-configuration");
        markMigrationCompleted();
        return;
      }

      String url = okapiUrl + MOD_CONFIGURATION_QUERY;

      HttpHeaders headers = new HttpHeaders();
      headers.add(XOkapiHeaders.TENANT, context.getTenantId());
      headers.add(XOkapiHeaders.TOKEN, context.getToken());
      headers.add(XOkapiHeaders.URL, okapiUrl);
      headers.add("Accept", "application/json");

      HttpEntity<String> entity = new HttpEntity<>(headers);

      ResponseEntity<String> response = restTemplate.exchange(
          url,
          HttpMethod.GET,
          entity,
          String.class
      );

      if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
        JsonNode root = objectMapper.readTree(response.getBody());
        JsonNode configs = root.get("configs");

        if (configs != null && configs.isArray() && configs.size() > 0) {
          log.info("Found {} configurations in mod-configuration", configs.size());

          for (JsonNode config : configs) {
            migrateConfiguration(config);
          }

          log.info("Successfully migrated configurations from mod-configuration");
        } else {
          log.info("No configurations found in mod-configuration, using defaults");
        }
        // Mark migration as completed
        markMigrationCompleted();
      }
    } catch (Exception e) {
      log.error("Error migrating configurations from mod-configuration", e);
      // Mark as completed anyway to prevent repeated attempts
      markMigrationCompleted();
    }
  }

  private void migrateConfiguration(JsonNode config) {
    try {
      String configName = config.get("configName").asText();
      String valueString = config.get("value").asText();
      boolean enabled = config.has("enabled") && config.get("enabled").asBoolean();

      if (!enabled) {
        log.debug("Skipping disabled configuration: {}", configName);
        return;
      }

      // Map old config names to new ones (e.g., "behavior" -> "behavioral")
      String mappedConfigName = CONFIG_NAME_MAPPING.getOrDefault(configName, configName);

      // Parse the value JSON string from mod-configuration
      JsonNode newValues = objectMapper.readTree(valueString);

      // Get existing config_value from configuration_settings table
      String selectSql = "SELECT config_value::text FROM " + context.getTenantId()
          + "_mod_oai_pmh.configuration_settings WHERE config_name = ?";

      String existingValueJson = jdbcTemplate.queryForObject(selectSql,
          String.class, mappedConfigName);

      if (existingValueJson != null) {
        // Parse existing JSONB value
        ObjectNode existingValues = (ObjectNode) objectMapper.readTree(existingValueJson);

        // Merge: override existing values with values from mod-configuration
        Map<String, String> changes = new HashMap<>();
        newValues.fields().forEachRemaining(entry -> {
          String key = entry.getKey();
          JsonNode newValue = entry.getValue();

          if (existingValues.has(key)) {
            String oldValue = existingValues.get(key).asText();
            String newValueStr = newValue.asText();
            if (!oldValue.equals(newValueStr)) {
              changes.put(key, oldValue + " -> " + newValueStr);
              existingValues.set(key, newValue);
            }
          } else {
            // New key not in defaults
            changes.put(key, "NEW: " + newValue.asText());
            existingValues.set(key, newValue);
          }
        });

        if (!changes.isEmpty()) {
          // Update the config_value with merged values
          String updateSql = "UPDATE " + context.getTenantId()
              + "_mod_oai_pmh.configuration_settings SET config_value = ?::jsonb "
              + "WHERE config_name = ?";

          String mergedJson = objectMapper.writeValueAsString(existingValues);
          jdbcTemplate.update(updateSql, mergedJson, mappedConfigName);

          log.info("Updated configuration '{}' with changes: {}", mappedConfigName, changes);
        } else {
          log.debug("No changes needed for configuration '{}'", mappedConfigName);
        }
      } else {
        log.warn("Configuration '{}' not found in configuration_settings table", mappedConfigName);
      }

    } catch (Exception e) {
      log.error("Error migrating individual configuration: {}", config, e);
    }
  }

  private void markMigrationCompleted() {
    try {
      String sql = "INSERT INTO " + context.getTenantId()
            + "_mod_oai_pmh.configuration_settings (id, config_name, config_value) "
          + "VALUES (gen_random_uuid(), '__migration_completed__', '{\"completed\": true}'"
          + "::jsonb) " + "ON CONFLICT (config_name) DO NOTHING";
      jdbcTemplate.update(sql);
      log.info("Marked configuration migration as completed");
    } catch (Exception e) {
      log.error("Error marking migration as completed", e);
    }
  }
}
