DROP VIEW IF EXISTS ${myuniversity}_${mymodule}.get_instances_with_marc_records CASCADE;
DROP VIEW IF EXISTS ${myuniversity}_${mymodule}.get_instances_with_marc_records_deleted CASCADE;
DROP VIEW IF EXISTS ${myuniversity}_${mymodule}.get_instances_from_inventory CASCADE;
DROP VIEW IF EXISTS ${myuniversity}_${mymodule}.get_instances_from_srs CASCADE;
DROP VIEW IF EXISTS ${myuniversity}_${mymodule}.get_instances_from_srs_deleted CASCADE;
DROP VIEW IF EXISTS ${myuniversity}_${mymodule}.get_marc_records CASCADE;
DROP VIEW IF EXISTS ${myuniversity}_${mymodule}.get_holdings CASCADE;
DROP VIEW IF EXISTS ${myuniversity}_${mymodule}.get_items CASCADE;
DROP VIEW IF EXISTS ${myuniversity}_${mymodule}.get_deleted_instances CASCADE;
DROP VIEW IF EXISTS ${myuniversity}_${mymodule}.get_deleted_holdings CASCADE;
DROP VIEW IF EXISTS ${myuniversity}_${mymodule}.get_deleted_items CASCADE;

CREATE OR REPLACE VIEW ${myuniversity}_${mymodule}.get_instances_from_inventory AS
SELECT * FROM ${myuniversity}_mod_inventory_storage.instance;

CREATE OR REPLACE VIEW ${myuniversity}_${mymodule}.get_instances_from_srs AS
SELECT * FROM ${myuniversity}_mod_source_record_storage.records_lb record_lb
WHERE record_lb.leader_record_status != 'd' AND record_lb.state = 'ACTUAL';

CREATE OR REPLACE VIEW ${myuniversity}_${mymodule}.get_instances_from_srs_deleted AS
SELECT * FROM ${myuniversity}_mod_source_record_storage.records_lb record_lb
WHERE (record_lb.leader_record_status = 'd' AND (record_lb.state = 'ACTUAL' OR record_lb.state = 'DELETED'))
 OR record_lb.state = 'DELETED';

CREATE OR REPLACE VIEW ${myuniversity}_${mymodule}.get_marc_records AS
SELECT * FROM ${myuniversity}_mod_source_record_storage.marc_records_lb;

CREATE OR REPLACE VIEW ${myuniversity}_${mymodule}.get_holdings AS
SELECT * FROM ${myuniversity}_mod_inventory_storage.holdings_record;

CREATE OR REPLACE VIEW ${myuniversity}_${mymodule}.get_items AS
SELECT * FROM ${myuniversity}_mod_inventory_storage.item;

CREATE OR REPLACE VIEW ${myuniversity}_${mymodule}.get_deleted_instances AS
SELECT * FROM ${myuniversity}_mod_inventory_storage.audit_instance;

CREATE OR REPLACE VIEW ${myuniversity}_${mymodule}.get_deleted_holdings AS
SELECT * FROM ${myuniversity}_mod_inventory_storage.audit_holdings_record;

CREATE OR REPLACE VIEW ${myuniversity}_${mymodule}.get_deleted_items AS
SELECT * FROM ${myuniversity}_mod_inventory_storage.audit_item;

CREATE OR REPLACE VIEW ${myuniversity}_${mymodule}.get_instances_with_marc_records
 AS
 SELECT instance_record.id AS instance_id,
           ( SELECT marc_record.content
           FROM ${myuniversity}_${mymodule}.get_instances_from_srs record_lb
             LEFT JOIN ${myuniversity}_${mymodule}.get_marc_records marc_record ON marc_record.id = record_lb.id
           WHERE instance_record.id = record_lb.external_id
           LIMIT 1) AS marc_record,
    instance_record.jsonb AS instance_record,
    instance_record.jsonb ->> 'source'::text AS source,
    instance_record.complete_updated_date AS instance_updated_date,
    ${myuniversity}_mod_inventory_storage.strtotimestamp((instance_record.jsonb -> 'metadata'::text) ->> 'createdDate'::text) AS instance_created_date,
           COALESCE(( SELECT record_lb.suppress_discovery
           FROM ${myuniversity}_${mymodule}.get_instances_from_srs record_lb
           WHERE instance_record.id = record_lb.external_id
           LIMIT 1), false) AS suppress_from_discovery_srs,
           COALESCE((instance_record.jsonb ->> 'discoverySuppress'::text)::boolean, false) AS suppress_from_discovery_inventory,
           false AS deleted
 FROM ${myuniversity}_${mymodule}.get_instances_from_inventory instance_record;

CREATE OR REPLACE VIEW ${myuniversity}_${mymodule}.get_instances_with_marc_records_deleted AS
SELECT (jsonb ->> 'id')::uuid                                                                                              instance_id,
      marc_record.content                                                                                                     marc_record,
      instance_record.jsonb                                                                                                   instance_record,
      instance_record.jsonb ->> 'source'                                                                                      source,
      ${myuniversity}_mod_inventory_storage.strToTimestamp(record_lb.updated_date::text)             instance_updated_date,
      ${myuniversity}_mod_inventory_storage.strToTimestamp(record_lb.created_date::text)             instance_created_date,
      COALESCE(record_lb.suppress_discovery, false)                                                                           suppress_from_discovery_srs,
      COALESCE((instance_record.jsonb ->> 'discoverySuppress')::bool, false)                                                  suppress_from_discovery_inventory,
      true                                                                                                                    deleted
      FROM ${myuniversity}_${mymodule}.get_instances_from_srs_deleted record_lb
      INNER JOIN ${myuniversity}_${mymodule}.get_instances_from_inventory instance_record
      ON instance_record.id = record_lb.external_id
      INNER JOIN ${myuniversity}_${mymodule}.get_marc_records marc_record  ON marc_record.id = record_lb.id
UNION ALL
SELECT (jsonb -> 'record' ->> 'id')::uuid                                                                                     instance_id,
      null                                                                                                                    marc_record,
      instance_record.jsonb                                                                                                   instance_record,
      instance_record.jsonb -> 'record' ->> 'source'                                                                          source,
      ${myuniversity}_mod_inventory_storage.strToTimestamp(instance_record.jsonb ->> 'createdDate') instance_updated_date,
      ${myuniversity}_mod_inventory_storage.strToTimestamp(instance_record.jsonb ->> 'createdDate') instance_created_date,
      false                                                                                                                   suppress_from_discovery_srs,
      COALESCE((instance_record.jsonb ->> 'discoverySuppress')::bool, false)                                                  suppress_from_discovery_inventory,
      true                                                                                                                    deleted
      FROM ${myuniversity}_${mymodule}.get_deleted_instances instance_record;

  GRANT ${myuniversity}_mod_inventory_storage TO ${myuniversity}_mod_oai_pmh;
  GRANT ${myuniversity}_mod_source_record_storage TO ${myuniversity}_mod_oai_pmh;
