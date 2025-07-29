CREATE OR REPLACE VIEW ${myuniversity}_${mymodule}.get_instances_from_inventory AS
SELECT * FROM ${myuniversity}_mod_inventory_storage.instance
WHERE jsonb ->> 'deleted' IS NULL OR jsonb ->> 'deleted' = 'false';