CREATE OR REPLACE VIEW ${myuniversity}_${mymodule}.get_instances_from_inventory AS
SELECT * FROM ${myuniversity}_mod_inventory_storage.instance;

CREATE OR REPLACE VIEW ${myuniversity}_${mymodule}.get_instances_from_srs AS
SELECT * FROM ${myuniversity}_mod_source_record_storage.records_lb;

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

CREATE OR REPLACE VIEW ${myuniversity}_${mymodule}.get_instances_with_marc_records AS
SELECT instance_record.id                                                                                         instance_id,
      marc_record.content                                                                                         marc_record,
      instance_record.jsonb                                                                                       instance_record,
      instance_record.jsonb ->> 'source'                                                                          source,
      ${myuniversity}_mod_inventory_storage.strToTimestamp(instance_record.jsonb -> 'metadata' ->> 'updatedDate') instance_updated_date,
      COALESCE(record_lb.suppress_discovery, false)                                                               suppress_from_discovery_srs,
      COALESCE((instance_record.jsonb ->> 'discoverySuppress')::bool, false)                                      suppress_from_discovery_inventory,
      false                                                                                                       deleted
      FROM ${myuniversity}_${mymodule}.get_instances_from_inventory instance_record
      LEFT JOIN ${myuniversity}_${mymodule}.get_instances_from_srs record_lb
      ON record_lb.external_id = instance_record.id
      LEFT JOIN ${myuniversity}_${mymodule}.get_marc_records marc_record ON marc_record.id = record_lb.id;

CREATE OR REPLACE VIEW ${myuniversity}_${mymodule}.get_instances_with_marc_records_deleted AS
SELECT (jsonb -> 'record' ->> 'id')::uuid                                                                                 instance_id,
      marc_record.content                                                                                         marc_record,
      instance_record.jsonb                                                                                       instance_record,
      instance_record.jsonb -> 'record' ->> 'source'                                                                          source,
      ${myuniversity}_mod_inventory_storage.strToTimestamp(instance_record.jsonb -> 'metadata' ->> 'updatedDate') instance_updated_date,
      COALESCE(record_lb.suppress_discovery, false)                                                               suppress_from_discovery_srs,
      COALESCE((instance_record.jsonb ->> 'discoverySuppress')::bool, false)                                      suppress_from_discovery_inventory,
      true                                                                                                        deleted
      FROM ${myuniversity}_${mymodule}.get_deleted_instances instance_record
      LEFT JOIN ${myuniversity}_${mymodule}.get_instances_from_srs record_lb
      ON record_lb.external_id = instance_record.id
      LEFT JOIN ${myuniversity}_${mymodule}.get_marc_records marc_record ON marc_record.id = record_lb.id;
