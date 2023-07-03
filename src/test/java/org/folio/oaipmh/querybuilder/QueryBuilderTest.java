package org.folio.oaipmh.querybuilder;

import static org.folio.oaipmh.Constants.ISO_UTC_DATE_TIME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.UUID;

class QueryBuilderTest {

  private final String testTenant = "test_tenant";

  @Test
  void testQueryBuilderNoTenant() {
    Throwable exception = assertThrows(QueryException.class,
      () -> QueryBuilder.build(null, null, null, null,
        RecordsSource.FOLIO, false, false, 1));
    assertEquals("tenant parameter cannot be null", exception.getMessage());
  }

  @Test
  void testQueryBuilderLimitLessThan1() {
    Throwable exception = assertThrows(QueryException.class,
      () -> QueryBuilder.build(testTenant, null, null, null,
        RecordsSource.FOLIO, false, false, 0));
    assertEquals("limit parameter must be greater than 0", exception.getMessage());
  }

  @Test
  void testQueryBuilderMinimumParameters() throws QueryException {
    var query = QueryBuilder.build(testTenant, null, null, null,
        RecordsSource.FOLIO, false, false, 1);
    var expected =
      "SELECT * FROM test_tenant_mod_oai_pmh.get_instances_with_marc_records inst\n" +
      "    WHERE inst.source = 'FOLIO'\n" +
      "ORDER BY instance_id\n" +
      "LIMIT 1;";
    assertEquals(expected, query);
  }

  @Test
  void testQueryBuilderLastInstanceId() throws QueryException {
    var lastInstanceId = UUID.randomUUID();
    var query = QueryBuilder.build(testTenant, lastInstanceId, null, null,
      RecordsSource.FOLIO, false, false, 1);
    var expected =
      String.format("SELECT * FROM test_tenant_mod_oai_pmh.get_instances_with_marc_records inst\n" +
        " WHERE inst.instance_id > '%s'::uuid\n" +
        "    AND inst.source = 'FOLIO'\n" +
        "ORDER BY instance_id\n" +
        "LIMIT 1;", lastInstanceId);
    assertEquals(expected, query);
  }

  @Test
  void testQueryBuilderFrom() throws QueryException {
    var from = new Date();
    var query = QueryBuilder.build(testTenant, null, from, null,
      RecordsSource.FOLIO, false, false, 1);
    var fromFormatted = ISO_UTC_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC)).format(from.toInstant());
    var expected =
      String.format("SELECT * FROM test_tenant_mod_oai_pmh.get_instances_with_marc_records inst\n" +
        "    WHERE inst.instance_updated_date >= test_tenant_mod_inventory_storage.dateOrMin(timestamptz '%s')\n" +
        "    AND inst.source = 'FOLIO'\n" +
        "    AND EXISTS (SELECT 1\n" +
        "              FROM test_tenant_mod_oai_pmh.get_holdings holdings_record\n" +
        "                       LEFT JOIN test_tenant_mod_oai_pmh.get_items item_record\n" +
        "                                 ON holdings_record.id = item_record.holdingsrecordid\n" +
        "                       LEFT JOIN test_tenant_mod_oai_pmh.get_deleted_holdings audit_holdings_record\n" +
        "                                 ON (audit_holdings_record.jsonb #>> '{record,instanceId}')::uuid = instance_id\n" +
        "                       LEFT JOIN test_tenant_mod_oai_pmh.get_deleted_items audit_item_record\n" +
        "                                 ON (audit_item_record.jsonb #>> '{record,holdingsRecordId}')::uuid =\n" +
        "                                    audit_holdings_record.id\n" +
        "                       LEFT JOIN test_tenant_mod_oai_pmh.get_deleted_items audit_item_record_deleted\n" +
        "                                 ON (audit_item_record_deleted.jsonb #>> '{record,holdingsRecordId}')::uuid =\n" +
        "                                    holdings_record.id\n" +
        "              WHERE instance_id = holdings_record.instanceid\n" +
        "                AND (test_tenant_mod_inventory_storage.strToTimestamp(holdings_record.jsonb -> 'metadata' ->> 'updatedDate')\n" +
        "                         BETWEEN test_tenant_mod_inventory_storage.dateOrMin(timestamptz '%s')\n" +
        "                         AND test_tenant_mod_inventory_storage.dateOrMax(null)\n" +
        "                  OR test_tenant_mod_inventory_storage.strToTimestamp(item_record.jsonb -> 'metadata' ->> 'updatedDate')\n" +
        "                         BETWEEN test_tenant_mod_inventory_storage.dateOrMin(timestamptz '%s')\n" +
        "                         AND test_tenant_mod_inventory_storage.dateOrMax(null)\n" +
        "                  OR test_tenant_mod_inventory_storage.strToTimestamp(audit_holdings_record.jsonb ->> 'createdDate')\n" +
        "                         BETWEEN test_tenant_mod_inventory_storage.dateOrMin(timestamptz '%s')\n" +
        "                         AND test_tenant_mod_inventory_storage.dateOrMax(null)\n" +
        "                  OR test_tenant_mod_inventory_storage.strToTimestamp(audit_item_record.jsonb ->> 'createdDate')\n" +
        "                         BETWEEN test_tenant_mod_inventory_storage.dateOrMin(timestamptz '%s')\n" +
        "                         AND test_tenant_mod_inventory_storage.dateOrMax(null)\n" +
        "                  OR test_tenant_mod_inventory_storage.strToTimestamp(audit_item_record_deleted.jsonb ->> 'createdDate')\n" +
        "                         BETWEEN test_tenant_mod_inventory_storage.dateOrMin(timestamptz '%s')\n" +
        "                         AND test_tenant_mod_inventory_storage.dateOrMax(null)\n" +
        "                  ))\n" +
        "ORDER BY instance_id\n" +
        "LIMIT 1;", fromFormatted, fromFormatted, fromFormatted, fromFormatted, fromFormatted, fromFormatted);
    assertEquals(expected, query);
  }

  @Test
  void testQueryBuilderUntil() throws QueryException {
    var until = new Date();
    var query = QueryBuilder.build(testTenant, null, null, until,
      RecordsSource.FOLIO, false, false, 1);
    var untilFormatted = ISO_UTC_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC)).format(until.toInstant());
    var expected =
      String.format("SELECT * FROM test_tenant_mod_oai_pmh.get_instances_with_marc_records inst\n" +
        "    WHERE inst.instance_updated_date <= test_tenant_mod_inventory_storage.dateOrMax(timestamptz '%s')\n" +
        "    AND inst.source = 'FOLIO'\n" +
        "    AND EXISTS (SELECT 1\n" +
        "              FROM test_tenant_mod_oai_pmh.get_holdings holdings_record\n" +
        "                       LEFT JOIN test_tenant_mod_oai_pmh.get_items item_record\n" +
        "                                 ON holdings_record.id = item_record.holdingsrecordid\n" +
        "                       LEFT JOIN test_tenant_mod_oai_pmh.get_deleted_holdings audit_holdings_record\n" +
        "                                 ON (audit_holdings_record.jsonb #>> '{record,instanceId}')::uuid = instance_id\n" +
        "                       LEFT JOIN test_tenant_mod_oai_pmh.get_deleted_items audit_item_record\n" +
        "                                 ON (audit_item_record.jsonb #>> '{record,holdingsRecordId}')::uuid =\n" +
        "                                    audit_holdings_record.id\n" +
        "                       LEFT JOIN test_tenant_mod_oai_pmh.get_deleted_items audit_item_record_deleted\n" +
        "                                 ON (audit_item_record_deleted.jsonb #>> '{record,holdingsRecordId}')::uuid =\n" +
        "                                    holdings_record.id\n" +
        "              WHERE instance_id = holdings_record.instanceid\n" +
        "                AND (test_tenant_mod_inventory_storage.strToTimestamp(holdings_record.jsonb -> 'metadata' ->> 'updatedDate')\n" +
        "                         BETWEEN test_tenant_mod_inventory_storage.dateOrMin(null)\n" +
        "                         AND test_tenant_mod_inventory_storage.dateOrMax(timestamptz '%s')\n" +
        "                  OR test_tenant_mod_inventory_storage.strToTimestamp(item_record.jsonb -> 'metadata' ->> 'updatedDate')\n" +
        "                         BETWEEN test_tenant_mod_inventory_storage.dateOrMin(null)\n" +
        "                         AND test_tenant_mod_inventory_storage.dateOrMax(timestamptz '%s')\n" +
        "                  OR test_tenant_mod_inventory_storage.strToTimestamp(audit_holdings_record.jsonb ->> 'createdDate')\n" +
        "                         BETWEEN test_tenant_mod_inventory_storage.dateOrMin(null)\n" +
        "                         AND test_tenant_mod_inventory_storage.dateOrMax(timestamptz '%s')\n" +
        "                  OR test_tenant_mod_inventory_storage.strToTimestamp(audit_item_record.jsonb ->> 'createdDate')\n" +
        "                         BETWEEN test_tenant_mod_inventory_storage.dateOrMin(null)\n" +
        "                         AND test_tenant_mod_inventory_storage.dateOrMax(timestamptz '%s')\n" +
        "                  OR test_tenant_mod_inventory_storage.strToTimestamp(audit_item_record_deleted.jsonb ->> 'createdDate')\n" +
        "                         BETWEEN test_tenant_mod_inventory_storage.dateOrMin(null)\n" +
        "                         AND test_tenant_mod_inventory_storage.dateOrMax(timestamptz '%s')\n" +
        "                  ))\n" +
        "ORDER BY instance_id\n" +
        "LIMIT 1;", untilFormatted, untilFormatted, untilFormatted, untilFormatted, untilFormatted, untilFormatted);
    assertEquals(expected, query);
  }
}
