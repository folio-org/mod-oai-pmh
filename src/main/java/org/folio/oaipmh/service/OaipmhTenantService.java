package org.folio.oaipmh.service;

import java.util.Map;
import lombok.extern.log4j.Log4j2;
import org.folio.spring.FolioExecutionContext;
import org.folio.spring.liquibase.FolioSpringLiquibase;
import org.folio.spring.service.TenantService;
import org.folio.tenant.domain.dto.TenantAttributes;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;


@Log4j2
@Service
@Primary
public class OaipmhTenantService extends TenantService {

  private final ConfigurationMigrationService configurationMigrationService;

  public OaipmhTenantService(JdbcTemplate jdbcTemplate, FolioExecutionContext context,
                             FolioSpringLiquibase folioSpringLiquibase,
                             ConfigurationMigrationService configurationMigrationService) {
    super(jdbcTemplate, context, folioSpringLiquibase);
    this.configurationMigrationService = configurationMigrationService;
  }


  @Override
  public synchronized void createOrUpdateTenant(TenantAttributes tenantAttributes) {
    this.folioSpringLiquibase.setChangeLogParameters(
        Map.of("tenant_id", this.context.getTenantId()));
    super.createOrUpdateTenant(tenantAttributes);

    // After schema is created and default values are inserted,
    // migrate customized configurations from mod-configuration
    log.info("Starting configuration migration from mod-configuration");
    configurationMigrationService.migrateConfigurationsFromModConfiguration();
    log.info("Configuration migration completed");
  }
}
