package org.folio.querybuilder;

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import static org.folio.oaipmh.Constants.ISO_UTC_DATE_TIME;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.UUID;

public class QueryBuilder {

  private static final Logger logger = LogManager.getLogger(QueryBuilder.class);

  private static final String QUERY = "SELECT * FROM %s_mod_oai_pmh.%s inst\n" +
    "%s" + // last instance id
    "%s" + // date from
    "%s" + // date until
    "%s" + // source
    "%s" + // discovery suppress
    "%s" + // deleted
    "ORDER BY instance_id\n" +
    "LIMIT %d;";

  private static final String AND_DATE_OR_MAX = "AND %s_mod_inventory_storage.dateOrMax(%s)";
  private static final String BETWEEN_DATE_OR_MIN = "BETWEEN %s_mod_inventory_storage.dateOrMin(%s)";
  private static final String INDENT = "                         ";

  private static final String DELETED = "   %s EXISTS (SELECT 1\n" +
    "              FROM %s_mod_oai_pmh.get_holdings holdings_record\n" +
    "                       LEFT JOIN %s_mod_oai_pmh.get_items item_record\n" +
    "                                 ON holdings_record.id = item_record.holdingsrecordid\n" +
    "                       LEFT JOIN %s_mod_oai_pmh.get_deleted_holdings audit_holdings_record\n" +
    "                                 ON (audit_holdings_record.jsonb #>> '{record,instanceId}')::uuid = instance_id\n" +
    "                       LEFT JOIN %s_mod_oai_pmh.get_deleted_items audit_item_record\n" +
    "                                 ON (audit_item_record.jsonb #>> '{record,holdingsRecordId}')::uuid =\n" +
    "                                    audit_holdings_record.id\n" +
    "                       LEFT JOIN %s_mod_oai_pmh.get_deleted_items audit_item_record_deleted\n" +
    "                                 ON (audit_item_record_deleted.jsonb #>> '{record,holdingsRecordId}')::uuid =\n" +
    "                                    holdings_record.id\n" +
    "              WHERE instance_id = holdings_record.instanceid\n" +
    "                AND (%s_mod_inventory_storage.strToTimestamp(holdings_record.jsonb -> 'metadata' ->> 'updatedDate')\n" +
    INDENT + BETWEEN_DATE_OR_MIN + "\n" +
    INDENT + AND_DATE_OR_MAX + "\n" +
    "                  OR %s_mod_inventory_storage.strToTimestamp(item_record.jsonb -> 'metadata' ->> 'updatedDate')\n" +
    INDENT + BETWEEN_DATE_OR_MIN + "\n" +
    INDENT + AND_DATE_OR_MAX + "\n" +
    "                  OR %s_mod_inventory_storage.strToTimestamp(audit_holdings_record.jsonb ->> 'createdDate')\n" +
    INDENT + BETWEEN_DATE_OR_MIN + "\n" +
    INDENT + AND_DATE_OR_MAX + "\n" +
    "                  OR %s_mod_inventory_storage.strToTimestamp(audit_item_record.jsonb ->> 'createdDate')\n" +
    INDENT + BETWEEN_DATE_OR_MIN + "\n" +
    INDENT + AND_DATE_OR_MAX + "\n" +
    "                  OR %s_mod_inventory_storage.strToTimestamp(audit_item_record_deleted.jsonb ->> 'createdDate')\n" +
    INDENT + BETWEEN_DATE_OR_MIN + "\n" +
    INDENT + AND_DATE_OR_MAX + "\n" +
    "                  ))\n";
  private static final String BASE_QUERY_NON_DELETED_TEMPLATE = "get_instances_with_marc_records";
  private static final String BASE_QUERY_DELETED_TEMPLATE = "get_instances_with_marc_records_deleted";
  private static final String DATE_UNTIL = "   %s inst.instance_updated_date <= %s_mod_inventory_storage.dateOrMax(timestamptz '%s')\n";
  private static final String DATE_FROM = "   %s inst.instance_updated_date >= %s_mod_inventory_storage.dateOrMin(timestamptz '%s')\n";
  private static final String DISCOVERY_SUPPRESS = "   %s coalesce(inst.suppress_from_discovery_srs, inst.suppress_from_discovery_inventory) = false\n";
  private static final String SOURCE = "   %s inst.source = '%s'\n";
  private static final String LAST_INSTANCE_ID = "%s inst.instance_id > '%s'::uuid\n";

  private static final String WHERE = " WHERE";

  private QueryBuilder() {}

  public static String build(String tenant, UUID lastInstanceId, Date from, Date until, RecordsSource source,
                             boolean discoverySuppress, boolean deletedRecords, int limit) throws QueryException {
    if (isNull(tenant)) {
      var errorMsg = "tenant parameter cannot be null";
      logger.error(errorMsg);
      throw new QueryException(errorMsg);
    }
    if (limit < 1) {
      var errorMsg = "limit parameter must be greater than 0";
      logger.error(errorMsg);
      throw new QueryException(errorMsg);
    }
    return format(QUERY, tenant,
      !deletedRecords ? BASE_QUERY_NON_DELETED_TEMPLATE : BASE_QUERY_DELETED_TEMPLATE,
      buildLastInstanceId(lastInstanceId),
      buildDateFrom(tenant, from, isNull(lastInstanceId)),
      buildDateUntil(tenant, until, isNull(lastInstanceId) && isNull(from)),
      buildSource(tenant, source, isNull(lastInstanceId) && isNull(from) && isNull(until)),
      buildSuppressFromDiscovery(discoverySuppress, isNull(lastInstanceId) && isNull(from) && isNull(until) && isNull(source)),
      buildDeleted(tenant, from, until, isNull(lastInstanceId) && isNull(from) && isNull(until) && isNull(source) && !discoverySuppress),
      limit);
  }

  private static String buildLastInstanceId(UUID lastInstanceId) {
    return nonNull(lastInstanceId) ? format(LAST_INSTANCE_ID, WHERE, lastInstanceId) : EMPTY;
  }

  private static String buildDateFrom(String tenant, Date from, boolean where) {
    var whereOrAnd = where ? WHERE : " AND";
    return nonNull(from) ? format(DATE_FROM, whereOrAnd, tenant, ISO_UTC_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC)).format(from.toInstant())) : EMPTY;
  }

  private static String buildDateUntil(String tenant, Date until, boolean where) {
    var whereOrAnd = where ? WHERE : " AND";
    return nonNull(until) ? format(DATE_UNTIL, whereOrAnd, tenant, ISO_UTC_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC)).format(until.toInstant())) : EMPTY;
  }

  private static String buildSource(String tenant, RecordsSource source, boolean where) {
    var whereOrAnd = where ? WHERE : " AND";
    return nonNull(source) ? format(SOURCE, whereOrAnd, tenant, source) : EMPTY;
  }

  private static String buildSuppressFromDiscovery(boolean discoverySuppress, boolean where) {
    var whereOrAnd = where ? WHERE : " AND";
    return !discoverySuppress ? format(DISCOVERY_SUPPRESS, whereOrAnd) : EMPTY;
  }

  private static String buildDeleted(String tenant, Date from, Date until, boolean where) {
    var whereOrAnd = where ? WHERE : " AND";
    return nonNull(from) || nonNull(until) ? format(DELETED, whereOrAnd,
      tenant, tenant, tenant, tenant, tenant,
      tenant, tenant, buildDate(from), tenant, buildDate(until),
      tenant, tenant, buildDate(from), tenant, buildDate(until),
      tenant, tenant, buildDate(from), tenant, buildDate(until),
      tenant, tenant, buildDate(from), tenant, buildDate(until),
      tenant, tenant, buildDate(from), tenant, buildDate(until)) : EMPTY;
  }

  private static String buildDate(Date date) {
    return isNull(date) ? null : format("timestamptz '%s'", ISO_UTC_DATE_TIME.withZone(ZoneId.from(ZoneOffset.UTC)).format(date.toInstant()));
  }
}
