CREATE OR REPLACE VIEW ${myuniversity}_${mymodule}.get_instances_from_srs_deleted AS
SELECT * FROM ${myuniversity}_mod_source_record_storage.records_lb record_lb
WHERE record_type = 'MARC_BIB'
  AND ((record_lb.leader_record_status = 'd' AND (record_lb.state = 'ACTUAL' OR record_lb.state = 'DELETED')) OR record_lb.state = 'DELETED');
