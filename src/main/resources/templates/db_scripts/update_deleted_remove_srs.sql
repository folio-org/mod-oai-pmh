CREATE OR REPLACE VIEW ${myuniversity}_${mymodule}.get_instances_with_marc_records_deleted AS
SELECT instance_record.id                                                                                                          instance_id,
      null::jsonb                                                                                                                  marc_record,
      instance_record.jsonb                                                                                                        instance_record,
      instance_record.jsonb ->> 'source'                                                                                           source,
      instance_record.complete_updated_date AS                                                                                     instance_updated_date,
      ${myuniversity}_mod_inventory_storage.strtotimestamp((instance_record.jsonb -> 'metadata'::text) ->> 'createdDate'::text) AS instance_created_date,
      COALESCE((instance_record.jsonb ->> 'discoverySuppress')::bool, false)                                                       suppress_from_discovery_srs,
      COALESCE((instance_record.jsonb ->> 'discoverySuppress')::bool, false)                                                       suppress_from_discovery_inventory,
      true                                                                                                                         deleted
      FROM ${myuniversity}_${mymodule}.get_instances_from_inventory_deleted instance_record;