package org.folio.oaipmh.helpers;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.oaipmh.Constants;
import org.folio.oaipmh.MetadataPrefix;
import org.folio.oaipmh.Request;
import org.openarchives.oai._2.ListMetadataFormatsType;
import org.openarchives.oai._2.MetadataFormatType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.OAIPMHerrorcodeType;
import org.openarchives.oai._2.ResumptionTokenType;

import java.util.Collections;
import java.util.List;

import javax.ws.rs.core.Response;

import static org.folio.oaipmh.Constants.INVALID_IDENTIFIER_ERROR_MESSAGE;

public class GetOaiMetadataFormatsHelper extends AbstractGetRecordsHelper {

  @Override
  public Future<javax.ws.rs.core.Response> handle(Request request, Context ctx) {
    if (request.getIdentifier() == null) {
      return Future.succeededFuture(retrieveMetadataFormatsWithNoIdentifier(request));
    }
    return super.handle(request, ctx);
  }

  @Override
  protected Future<javax.ws.rs.core.Response> processRecords(Context ctx, Request request, JsonObject srsRecords) {
    JsonArray instances = storageHelper.getItems(srsRecords);
    Response response;
    if (instances != null && !instances.isEmpty()) {
      response = retrieveMetadataFormatsWithNoIdentifier(request);
    } else {
      response = buildIdentifierNotFoundResponse(request);
    }
    return Future.succeededFuture(response);
  }

  @Override
  protected List<OAIPMHerrorType> validateRequest(Request request) {
    if (!validateIdentifier(request)) {
      OAIPMHerrorType error = new OAIPMHerrorType().withCode(OAIPMHerrorcodeType.BAD_ARGUMENT)
        .withValue(INVALID_IDENTIFIER_ERROR_MESSAGE);
      return List.of(error);
    }
    return Collections.emptyList();
  }

  @Override
  protected void addResumptionTokenToOaiResponse(OAIPMH oaipmh, ResumptionTokenType resumptionToken) {
    if (resumptionToken != null) {
      throw new UnsupportedOperationException("Control flow is not applicable for ListMetadataFormats verb.");
    }
  }

  /**
   * Processes request without identifier
   * @return future with {@link OAIPMH} response
   */
  private javax.ws.rs.core.Response retrieveMetadataFormatsWithNoIdentifier(Request request) {
    OAIPMH oaipmh = getResponseHelper().buildBaseOaipmhResponse(request).withListMetadataFormats(getMetadataFormatTypes());
    return getResponseHelper().buildSuccessResponse(oaipmh);
  }

  /**
   * Builds {@linkplain javax.ws.rs.core.Response Response} with 'id-does-not-exist' error because passed identifier isn't exist.
   * @return {@linkplain javax.ws.rs.core.Response Response}  with {@link OAIPMH} response
   */
  private javax.ws.rs.core.Response buildIdentifierNotFoundResponse(Request request) {
    OAIPMH oaipmh = getResponseHelper().buildOaipmhResponseWithErrors(request, OAIPMHerrorcodeType.ID_DOES_NOT_EXIST, Constants.RECORD_NOT_FOUND_ERROR);
    return getResponseHelper().buildFailureResponse(oaipmh, request);
  }

  /**
   * Creates ListMetadataFormatsType of supported metadata formats
   * @return supported metadata formats
   */
  private ListMetadataFormatsType getMetadataFormatTypes() {
    ListMetadataFormatsType mft = new ListMetadataFormatsType();
    for (MetadataPrefix mp : MetadataPrefix.values()) {
      mft.withMetadataFormats(new MetadataFormatType()
        .withMetadataPrefix(mp.getName())
        .withSchema(mp.getSchema())
        .withMetadataNamespace(mp.getMetadataNamespace()));
    }
    return mft;
  }
}
