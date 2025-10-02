package org.folio.oaipmh.querybuilder;

import static org.folio.oaipmh.Constants.ISO_UTC_DATE_TIME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.UUID;
import org.junit.jupiter.api.Test;

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
        "SELECT * FROM test_tenant_mod_oai_pmh.get_instances_with_marc_records inst\n"
        + "    WHERE ( inst.source = 'FOLIO'\n"
        + " OR inst.source = 'CONSORTIUM-FOLIO' OR inst.source = 'LINKED_DATA' OR inst.source"
        + " = 'CONSORTIUM-LINKED_DATA') ORDER BY instance_id\nLIMIT 1;";
    assertEquals(expected, query);
  }

  @Test
  void testQueryBuilderLastInstanceId() throws QueryException {
    var lastInstanceId = UUID.randomUUID().toString();
    var query = QueryBuilder.build(testTenant, lastInstanceId, null, null,
        RecordsSource.FOLIO, false, false, 1);
    var expected =
        String.format("SELECT * FROM test_tenant_mod_oai_pmh.get_instances_with_marc_records inst"
            + "\n WHERE inst.instance_id > '%s'::uuid\n"
            + "    AND ( inst.source = 'FOLIO'\n"
            + " OR inst.source = 'CONSORTIUM-FOLIO' OR inst.source = 'LINKED_DATA' OR inst.source"
            + " = 'CONSORTIUM-LINKED_DATA') "
            + "ORDER BY instance_id\n"
            + "LIMIT 1;", lastInstanceId);
    assertEquals(expected, query);
  }

  @Test
  void testQueryBuilderFrom() throws QueryException {
    var from = new Date();
    var fromFormatted = ISO_UTC_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC))
        .format(from.toInstant());
    var query = QueryBuilder.build(testTenant, null, fromFormatted, null,
        RecordsSource.FOLIO, false, false, 1);

    var expected =
        String.format("SELECT * FROM test_tenant_mod_oai_pmh.get_instances_with_marc_records inst"
            + "\n    WHERE ( inst.source = 'FOLIO'\n"
            + " OR inst.source = 'CONSORTIUM-FOLIO' OR inst.source = 'LINKED_DATA' OR inst.source"
            + " = 'CONSORTIUM-LINKED_DATA') "
            + "    AND inst.instance_updated_date >= test_tenant_mod_inventory_storage.dateOrMin"
            + "(timestamptz '%s')\n"
            + "ORDER BY instance_id\n"
            + "LIMIT 1;", fromFormatted, fromFormatted, fromFormatted, fromFormatted,
            fromFormatted, fromFormatted);
    assertEquals(expected, query);
  }

  @Test
  void testQueryBuilderUntil() throws QueryException {
    var until = new Date();
    var untilFormatted = ISO_UTC_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC))
        .format(until.toInstant());
    var query = QueryBuilder.build(testTenant, null, null, untilFormatted,
        RecordsSource.FOLIO, false, false, 1);

    var expected =
        String.format("SELECT * FROM test_tenant_mod_oai_pmh.get_instances_with_marc_records inst"
            + "\n    WHERE ( inst.source = 'FOLIO'\n"
            + " OR inst.source = 'CONSORTIUM-FOLIO' OR inst.source = 'LINKED_DATA' OR inst.source"
            + " = 'CONSORTIUM-LINKED_DATA') "
            + "    AND inst.instance_updated_date <= test_tenant_mod_inventory_storage.dateOrMax"
            + "(timestamptz '%s')\n"
            + "ORDER BY instance_id\n"
            + "LIMIT 1;", untilFormatted, untilFormatted, untilFormatted, untilFormatted,
            untilFormatted, untilFormatted);
    assertEquals(expected, query);
  }
}
