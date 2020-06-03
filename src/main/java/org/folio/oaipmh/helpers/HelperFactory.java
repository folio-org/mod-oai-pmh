package org.folio.oaipmh.helpers;

import org.openarchives.oai._2.VerbType;

import io.vertx.core.Context;
import io.vertx.ext.web.RoutingContext;

public class HelperFactory {

  public static VerbHelper createVerbHelper(VerbType verbType, Context vertxContext, RoutingContext routingContext) {
    VerbHelper vh = null;
    switch (verbType) {
      case LIST_RECORDS:
        vh = new GetOaiRecordsHelper(verbType, vertxContext, routingContext);
        break;
      case IDENTIFY:
        vh = new GetOaiRepositoryInfoHelper();
        break;
/      case LIST_IDENTIFIERS:
        vh = new GetOaiIdentifiersHelper();
//        break;
//      case LIST_SETS:
//        vh = new GetOaiSetsHelper();
//        break;
//      case LIST_METADATA_FORMATS:
//        vh = new GetOaiMetadataFormatsHelper();
//        break;
//      case GET_RECORD:
//        vh = new GetOaiRecordHelper();
//        break;
    }
    return vh;
  }


  VerbHelper createVerbHelper(VerbType vt, String metadataPrefix) {
    return null;
  }

}
