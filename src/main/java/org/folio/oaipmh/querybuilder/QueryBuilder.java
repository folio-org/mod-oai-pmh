package org.folio.oaipmh.querybuilder;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.EMPTY;

public class QueryBuilder {

  private static final Logger logger = LogManager.getLogger(QueryBuilder.class);

  private static final String MAX_DATE = "2050-12-31";
  private static final String MIN_DATE = "1970-01-01";

  private static final String QUERY = "SELECT * FROM %s_mod_oai_pmh.%s inst\n" +
    "%s" + // last instance id
    "%s" + // discovery suppress
    "%s" + // source
    "%s" + // deleted
    "%s" + // date from
    "%s" + // date until
    "ORDER BY instance_id\n" +
    "LIMIT %d;";

  private static final String DELETED_INSTANCES = " %s %s_mod_inventory_storage.strToTimestamp\n" +
    " (instance_created_date::text) >= %s_mod_inventory_storage.dateOrMin(timestamptz '%s')\n" +
    " AND \n" +
    " %s_mod_inventory_storage.strToTimestamp(instance_created_date::text) <= %s_mod_inventory_storage.dateOrMax(timestamptz '%s')\n";

  private static final String BASE_QUERY_NON_DELETED_TEMPLATE = "get_instances_with_marc_records";
  private static final String BASE_QUERY_DELETED_TEMPLATE = "get_instances_with_marc_records_deleted";
  private static final String DATE_UNTIL_FOLIO = "   %s inst.instance_updated_date <= %s_mod_inventory_storage.dateOrMax(timestamptz '%s')\n";
  private static final String DATE_FROM_FOLIO = "   %s inst.instance_updated_date >= %s_mod_inventory_storage.dateOrMin(timestamptz '%s')\n";
  private static final String DISCOVERY_SUPPRESS = "   %s COALESCE(inst.suppress_from_discovery_srs, false) = false AND COALESCE(inst.suppress_from_discovery_inventory, false) = false\n";
  private static final String SOURCE = "   %s inst.source = '%s'\n";
  private static final String LAST_INSTANCE_ID = "%s inst.instance_id > '%s'::uuid\n";

  private static final String WHERE = " WHERE";

  private QueryBuilder() {}

  public static String build(String tenant, String lastInstanceId, String from, String until, RecordsSource source,
                             boolean skipSuppressedFromDiscovery, boolean deletedRecords, int limit) throws QueryException {
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
    return format(QUERY,
      tenant,
      !deletedRecords ? BASE_QUERY_NON_DELETED_TEMPLATE : BASE_QUERY_DELETED_TEMPLATE,
      buildLastInstanceId(lastInstanceId),
      buildSuppressFromDiscovery(skipSuppressedFromDiscovery, isNull(lastInstanceId)),
      buildSource(source, isNull(lastInstanceId) && !skipSuppressedFromDiscovery),
      buildDateFrom(tenant, from, isNull(lastInstanceId)  && !skipSuppressedFromDiscovery && isNull(source), deletedRecords),
      buildDateUntil(tenant, until, isNull(lastInstanceId)  && !skipSuppressedFromDiscovery && isNull(source) && isNull(from), deletedRecords),
      buildDeleted(tenant, from, until, isNull(lastInstanceId)  && !skipSuppressedFromDiscovery && isNull(source), deletedRecords ),
      limit);
  }

  private static String buildLastInstanceId(String lastInstanceId) {
    var where = WHERE;
    return nonNull(lastInstanceId) ? format(LAST_INSTANCE_ID, where, lastInstanceId) : EMPTY;
  }

  private static String buildDateFrom(String tenant, String from, boolean where, boolean deletedSupport) {
    if (nonNull(from) && !deletedSupport) {
      var whereOrAnd = where ? WHERE : " AND";
      var dateFromTemplate = DATE_FROM_FOLIO;
      return format(dateFromTemplate, whereOrAnd, tenant, from);
    }
    return EMPTY;
  }

  private static String buildDateUntil(String tenant, String until, boolean where, boolean deletedSupport) {
    if (nonNull(until) && !deletedSupport) {
      var whereOrAnd = where ? WHERE : " AND";
      var dateUntilTemplate = DATE_UNTIL_FOLIO;
      return format(dateUntilTemplate, whereOrAnd, tenant, until);
    }
    return EMPTY;
  }

  private static String buildSource(RecordsSource source, boolean where) {
    if (nonNull(source)) {
      var whereOrAnd = where ? WHERE : " AND";
      String sql;
      if (source == RecordsSource.MARC) {
        sql = format(SOURCE + " OR inst.source = '%s' OR inst.source = '%s' OR inst.source = '%s') ", whereOrAnd + " (", source,
          RecordsSource.MARC_SHARED, RecordsSource.CONSORTIUM_MARC, RecordsSource.LINKED_DATA);
      } else {
        sql = format(SOURCE + " OR inst.source = '%s') ", whereOrAnd + " (", source, RecordsSource.CONSORTIUM_FOLIO);
      }
      return sql;
    }
    return EMPTY;
  }

  private static String buildSuppressFromDiscovery(boolean skipSuppressedFromDiscovery, boolean where) {
    var whereOrAnd = where ? WHERE : " AND";
    return skipSuppressedFromDiscovery ? format(DISCOVERY_SUPPRESS, whereOrAnd) : EMPTY;
  }

  private static String buildDeleted(String tenant, String from, String until, boolean where, boolean deletedSupport) {
    if ((nonNull(from) || nonNull(until)) && deletedSupport) {
      if (isNull(from)) {
        from = MIN_DATE;
      }
      if (isNull(until)) {
        until = MAX_DATE;
      }
      var whereOrAnd = where ? WHERE : " AND";
      return format(DELETED_INSTANCES, whereOrAnd, tenant, tenant, from, tenant, tenant, until);
    }
    return EMPTY;
  }
}
