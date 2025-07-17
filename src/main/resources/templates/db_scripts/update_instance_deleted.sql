CREATE OR REPLACE VIEW ${myuniversity}_${mymodule}.get_instances_from_inventory AS
SELECT * FROM ${myuniversity}_mod_inventory_storage.instance
WHERE jsonb ->> 'deleted' = 'false';

CREATE OR REPLACE VIEW ${myuniversity}_${mymodule}.get_instances_from_inventory_deleted AS
SELECT * FROM ${myuniversity}_mod_inventory_storage.instance
WHERE jsonb ->> 'deleted' = 'true';

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
      FROM ${myuniversity}_${mymodule}.get_deleted_instances instance_record
UNION ALL
SELECT instance_record.id                                                                                                          instance_id,
      null                                                                                                                         marc_record,
      instance_record.jsonb                                                                                                        instance_record,
      instance_record.jsonb ->> 'source'                                                                                           source,
      instance_record.complete_updated_date AS                                                                                     instance_updated_date,
      ${myuniversity}_mod_inventory_storage.strtotimestamp((instance_record.jsonb -> 'metadata'::text) ->> 'createdDate'::text) AS instance_created_date,
      false                                                                                                                        suppress_from_discovery_srs,
      COALESCE((instance_record.jsonb ->> 'discoverySuppress')::bool, false)                                                       suppress_from_discovery_inventory,
      true                                                                                                                         deleted
      FROM ${myuniversity}_${mymodule}.get_instances_from_inventory_deleted instance_record;
