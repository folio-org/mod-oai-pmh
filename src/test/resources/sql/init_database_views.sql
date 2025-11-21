-- Setup

-- CREATE SCHEMA oaitest_mod_oai_pmh;
ALTER SCHEMA oaitest_mod_oai_pmh OWNER TO oaitest_mod_oai_pmh;

-- create tables
CREATE TABLE oaitest_mod_oai_pmh.rmb_internal (
    id integer NOT NULL,
    jsonb jsonb NOT NULL
);

ALTER TABLE oaitest_mod_oai_pmh.rmb_internal OWNER TO folio_admin;

-- create views
DROP VIEW IF EXISTS oaitest_mod_oai_pmh.get_instances_from_inventory CASCADE;
DROP VIEW IF EXISTS oaitest_mod_oai_pmh.get_instances_from_srs CASCADE;
DROP VIEW IF EXISTS oaitest_mod_oai_pmh.get_instances_from_srs_deleted CASCADE;
DROP VIEW IF EXISTS oaitest_mod_oai_pmh.get_marc_records CASCADE;
DROP VIEW IF EXISTS oaitest_mod_oai_pmh.get_holdings CASCADE;
DROP VIEW IF EXISTS oaitest_mod_oai_pmh.get_items CASCADE;
DROP VIEW IF EXISTS oaitest_mod_oai_pmh.get_deleted_instances CASCADE;
DROP VIEW IF EXISTS oaitest_mod_oai_pmh.get_deleted_holdings CASCADE;
DROP VIEW IF EXISTS oaitest_mod_oai_pmh.get_deleted_items CASCADE;
DROP VIEW IF EXISTS oaitest_mod_oai_pmh.get_instances_with_marc_records CASCADE;
DROP VIEW IF EXISTS oaitest_mod_oai_pmh.get_instances_with_marc_records_deleted CASCADE;

CREATE OR REPLACE VIEW oaitest_mod_oai_pmh.get_instances_from_inventory AS
SELECT * FROM oaitest_mod_inventory_storage.instance;

CREATE OR REPLACE VIEW oaitest_mod_oai_pmh.get_instances_from_srs AS
SELECT * FROM oaitest_mod_source_record_storage.records_lb record_lb
WHERE record_lb.leader_record_status != 'd' AND record_lb.state = 'ACTUAL';

CREATE OR REPLACE VIEW oaitest_mod_oai_pmh.get_instances_from_srs_deleted AS
SELECT * FROM oaitest_mod_source_record_storage.records_lb record_lb
WHERE (record_lb.leader_record_status = 'd' AND (record_lb.state = 'ACTUAL' OR record_lb.state = 'DELETED')) OR record_lb.state = 'DELETED';

CREATE OR REPLACE VIEW oaitest_mod_oai_pmh.get_marc_records AS
SELECT * FROM oaitest_mod_source_record_storage.marc_records_lb;

CREATE OR REPLACE VIEW oaitest_mod_oai_pmh.get_holdings AS
SELECT * FROM oaitest_mod_inventory_storage.holdings_record;

CREATE OR REPLACE VIEW oaitest_mod_oai_pmh.get_items AS
SELECT * FROM oaitest_mod_inventory_storage.item;

CREATE OR REPLACE VIEW oaitest_mod_oai_pmh.get_deleted_instances AS
SELECT * FROM oaitest_mod_inventory_storage.audit_instance;

CREATE OR REPLACE VIEW oaitest_mod_oai_pmh.get_deleted_holdings AS
SELECT * FROM oaitest_mod_inventory_storage.audit_holdings_record;

CREATE OR REPLACE VIEW oaitest_mod_oai_pmh.get_deleted_items AS
SELECT * FROM oaitest_mod_inventory_storage.audit_item;

CREATE OR REPLACE VIEW oaitest_mod_oai_pmh.get_instances_with_marc_records AS
SELECT instance_record.id                                                                                     instance_id,
       (select marc_record.content
        from oaitest_mod_oai_pmh.get_instances_from_srs record_lb
                 LEFT JOIN oaitest_mod_oai_pmh.get_marc_records marc_record ON marc_record.id = record_lb.id
        where instance_record.id = record_lb.external_id limit 1)                                                     marc_record,
       instance_record.jsonb                                                                                  instance_record,
       instance_record.jsonb ->> 'source'                                                                     source,
       oaitest_mod_inventory_storage.strToTimestamp(instance_record.jsonb -> 'metadata' ->> 'updatedDate') instance_updated_date,
       oaitest_mod_inventory_storage.strToTimestamp(instance_record.jsonb -> 'metadata' ->> 'createdDate') instance_created_date,
       (select oaitest_mod_inventory_storage.strToTimestamp(record_lb.updated_date::text)
        from oaitest_mod_oai_pmh.get_instances_from_srs record_lb
        where instance_record.id = record_lb.external_id limit 1)                                                     marc_updated_date,
       (select oaitest_mod_inventory_storage.strToTimestamp(record_lb.created_date::text)
        from oaitest_mod_oai_pmh.get_instances_from_srs record_lb
        where instance_record.id = record_lb.external_id limit 1)                                                     marc_created_date,
       COALESCE((select record_lb.suppress_discovery
                 from oaitest_mod_oai_pmh.get_instances_from_srs record_lb
                 where instance_record.id = record_lb.external_id limit 1),
                false)                                                                                        suppress_from_discovery_srs,
       COALESCE((instance_record.jsonb ->> 'discoverySuppress')::bool,
                false)                                                                                        suppress_from_discovery_inventory,
       false                                                                                                  deleted
FROM oaitest_mod_oai_pmh.get_instances_from_inventory instance_record;

CREATE OR REPLACE VIEW oaitest_mod_oai_pmh.get_instances_with_marc_records_from_until AS
 SELECT instance_record.id                                                                                         instance_id,
       marc_record.content                                                                                         marc_record,
       instance_record.jsonb                                                                                       instance_record,
       instance_record.jsonb ->> 'source'                                                                          source,
       oaitest_mod_inventory_storage.strToTimestamp(instance_record.jsonb -> 'metadata' ->> 'updatedDate') instance_updated_date,
       oaitest_mod_inventory_storage.strToTimestamp(instance_record.jsonb -> 'metadata' ->> 'createdDate') instance_created_date,
       oaitest_mod_inventory_storage.strToTimestamp(record_lb.updated_date::text)                          marc_updated_date,
       oaitest_mod_inventory_storage.strToTimestamp(record_lb.created_date::text)                          marc_created_date,
       COALESCE(record_lb.suppress_discovery, false)                                                               suppress_from_discovery_srs,
       COALESCE((instance_record.jsonb ->> 'discoverySuppress')::bool, false)                                      suppress_from_discovery_inventory,
       false                                                                                                       deleted
       FROM oaitest_mod_oai_pmh.get_instances_from_inventory instance_record
       LEFT JOIN oaitest_mod_oai_pmh.get_instances_from_srs record_lb
       ON instance_record.id = record_lb.external_id
       LEFT JOIN oaitest_mod_oai_pmh.get_marc_records marc_record
       ON marc_record.id = record_lb.id;

CREATE OR REPLACE VIEW oaitest_mod_oai_pmh.get_instances_with_marc_records_deleted AS
SELECT (jsonb ->> 'id')::uuid                                                                                              instance_id,
      marc_record.content                                                                                                     marc_record,
      instance_record.jsonb                                                                                                   instance_record,
      instance_record.jsonb ->> 'source'                                                                                      source,
      oaitest_mod_inventory_storage.strToTimestamp(record_lb.updated_date::text)             instance_updated_date,
      oaitest_mod_inventory_storage.strToTimestamp(record_lb.created_date::text)             instance_created_date,
      COALESCE(record_lb.suppress_discovery, false)                                                                           suppress_from_discovery_srs,
      COALESCE((instance_record.jsonb ->> 'discoverySuppress')::bool, false)                                                  suppress_from_discovery_inventory,
      true                                                                                                                    deleted
      FROM oaitest_mod_oai_pmh.get_instances_from_srs_deleted record_lb
      INNER JOIN oaitest_mod_oai_pmh.get_instances_from_inventory instance_record
      ON instance_record.id = record_lb.external_id
      INNER JOIN oaitest_mod_oai_pmh.get_marc_records marc_record  ON marc_record.id = record_lb.id
UNION ALL
SELECT (jsonb -> 'record' ->> 'id')::uuid                                                                                     instance_id,
      null                                                                                                                    marc_record,
      instance_record.jsonb                                                                                                   instance_record,
      instance_record.jsonb -> 'record' ->> 'source'                                                                          source,
      oaitest_mod_inventory_storage.strToTimestamp(instance_record.jsonb ->> 'createdDate') instance_updated_date,
      oaitest_mod_inventory_storage.strToTimestamp(instance_record.jsonb ->> 'createdDate') instance_created_date,
      false                                                                                                                   suppress_from_discovery_srs,
      COALESCE((instance_record.jsonb ->> 'discoverySuppress')::bool, false)                                                  suppress_from_discovery_inventory,
      true                                                                                                                    deleted
      FROM oaitest_mod_oai_pmh.get_deleted_instances instance_record;

CREATE OR REPLACE VIEW oaitest_mod_oai_pmh.get_instances_from_inventory AS
SELECT * FROM oaitest_mod_inventory_storage.instance
WHERE jsonb ->> 'deleted' = 'false';

CREATE OR REPLACE VIEW oaitest_mod_oai_pmh.get_instances_from_inventory_deleted AS
SELECT * FROM oaitest_mod_inventory_storage.instance
WHERE jsonb ->> 'deleted' = 'true';

CREATE OR REPLACE VIEW oaitest_mod_oai_pmh.get_instances_with_marc_records_deleted AS
SELECT (jsonb ->> 'id')::uuid                                                                                              instance_id,
      marc_record.content                                                                                                     marc_record,
      instance_record.jsonb                                                                                                   instance_record,
      instance_record.jsonb ->> 'source'                                                                                      source,
      oaitest_mod_inventory_storage.strToTimestamp(record_lb.updated_date::text)             instance_updated_date,
      oaitest_mod_inventory_storage.strToTimestamp(record_lb.created_date::text)             instance_created_date,
      COALESCE(record_lb.suppress_discovery, false)                                                                           suppress_from_discovery_srs,
      COALESCE((instance_record.jsonb ->> 'discoverySuppress')::bool, false)                                                  suppress_from_discovery_inventory,
      true                                                                                                                    deleted
      FROM oaitest_mod_oai_pmh.get_instances_from_srs_deleted record_lb
      INNER JOIN oaitest_mod_oai_pmh.get_instances_from_inventory instance_record
      ON instance_record.id = record_lb.external_id
      INNER JOIN oaitest_mod_oai_pmh.get_marc_records marc_record  ON marc_record.id = record_lb.id
UNION ALL
SELECT (jsonb -> 'record' ->> 'id')::uuid                                                                                     instance_id,
      null                                                                                                                    marc_record,
      instance_record.jsonb                                                                                                   instance_record,
      instance_record.jsonb -> 'record' ->> 'source'                                                                          source,
      oaitest_mod_inventory_storage.strToTimestamp(instance_record.jsonb ->> 'createdDate') instance_updated_date,
      oaitest_mod_inventory_storage.strToTimestamp(instance_record.jsonb ->> 'createdDate') instance_created_date,
      false                                                                                                                   suppress_from_discovery_srs,
      COALESCE((instance_record.jsonb ->> 'discoverySuppress')::bool, false)                                                  suppress_from_discovery_inventory,
      true                                                                                                                    deleted
      FROM oaitest_mod_oai_pmh.get_deleted_instances instance_record
UNION ALL
SELECT instance_record.id                                                                                                          instance_id,
      null                                                                                                                         marc_record,
      instance_record.jsonb                                                                                                        instance_record,
      instance_record.jsonb ->> 'source'                                                                                           source,
      instance_record.complete_updated_date AS                                                                                     instance_updated_date,
      oaitest_mod_inventory_storage.strtotimestamp((instance_record.jsonb -> 'metadata'::text) ->> 'createdDate'::text) AS instance_created_date,
      false                                                                                                                        suppress_from_discovery_srs,
      COALESCE((instance_record.jsonb ->> 'discoverySuppress')::bool, false)                                                       suppress_from_discovery_inventory,
      true                                                                                                                         deleted
      FROM oaitest_mod_oai_pmh.get_instances_from_inventory_deleted instance_record;

CREATE OR REPLACE VIEW oaitest_mod_oai_pmh.get_instances_with_marc_records_deleted AS
SELECT (jsonb ->> 'id')::uuid                                                                                              instance_id,
      marc_record.content                                                                                                     marc_record,
      instance_record.jsonb                                                                                                   instance_record,
      instance_record.jsonb ->> 'source'                                                                                      source,
      oaitest_mod_inventory_storage.strToTimestamp(record_lb.updated_date::text)             instance_updated_date,
      oaitest_mod_inventory_storage.strToTimestamp(record_lb.created_date::text)             instance_created_date,
      COALESCE(record_lb.suppress_discovery, false)                                                                           suppress_from_discovery_srs,
      COALESCE((instance_record.jsonb ->> 'discoverySuppress')::bool, false)                                                  suppress_from_discovery_inventory,
      true                                                                                                                    deleted
      FROM oaitest_mod_oai_pmh.get_instances_from_srs_deleted record_lb
      INNER JOIN oaitest_mod_oai_pmh.get_instances_from_inventory instance_record
      ON instance_record.id = record_lb.external_id
      INNER JOIN oaitest_mod_oai_pmh.get_marc_records marc_record  ON marc_record.id = record_lb.id
UNION ALL
SELECT instance_record.id                                                                                                          instance_id,
      null                                                                                                                         marc_record,
      instance_record.jsonb                                                                                                        instance_record,
      instance_record.jsonb ->> 'source'                                                                                           source,
      instance_record.complete_updated_date AS                                                                                     instance_updated_date,
      oaitest_mod_inventory_storage.strtotimestamp((instance_record.jsonb -> 'metadata'::text) ->> 'createdDate'::text) AS instance_created_date,
      false                                                                                                                        suppress_from_discovery_srs,
      COALESCE((instance_record.jsonb ->> 'discoverySuppress')::bool, false)                                                       suppress_from_discovery_inventory,
      true                                                                                                                         deleted
      FROM oaitest_mod_oai_pmh.get_instances_from_inventory_deleted instance_record;

CREATE OR REPLACE VIEW oaitest_mod_oai_pmh.get_instances_from_inventory AS
SELECT * FROM oaitest_mod_inventory_storage.instance
WHERE jsonb ->> 'deleted' IS NULL OR jsonb ->> 'deleted' = 'false';

DROP VIEW IF EXISTS oaitest_mod_oai_pmh.get_deleted_instances CASCADE;
DROP VIEW IF EXISTS oaitest_mod_oai_pmh.get_deleted_holdings CASCADE;
DROP VIEW IF EXISTS oaitest_mod_oai_pmh.get_deleted_items CASCADE;
