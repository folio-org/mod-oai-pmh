package org.folio.oaipmh.helpers;

import static org.folio.oaipmh.Constants.INVALID_IDENTIFIER_ERROR_MESSAGE;
import static org.folio.oaipmh.Constants.REPOSITORY_MAX_RECORDS_PER_RESPONSE;
import static org.folio.oaipmh.Constants.REPOSITORY_SUPPRESSED_RECORDS_PROCESSING;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getBooleanProperty;

import java.util.Date;

import org.folio.oaipmh.Constants;
import org.folio.oaipmh.MetadataPrefix;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.helpers.response.ResponseHelper;
import org.folio.rest.client.SourceStorageSourceRecordsClient;
import org.folio.rest.tools.client.Response;
import org.openarchives.oai._2.ListMetadataFormatsType;
import org.openarchives.oai._2.MetadataFormatType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorcodeType;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;


public class GetOaiMetadataFormatsHelper extends AbstractHelper {

  private static final Logger logger = LoggerFactory.getLogger(GetOaiMetadataFormatsHelper.class);

  @Override
  public Future<javax.ws.rs.core.Response> handle(Request request, Context ctx) {
    if (request.getIdentifier() == null) {
      return Future.succeededFuture(retrieveMetadataFormatsWithNoIdentifier(request));
    } else if (!validateIdentifier(request)) {
      return Future.succeededFuture(buildBadArgumentResponse(request));
    }

    Promise<javax.ws.rs.core.Response> promise = Promise.promise();
    try {
      final boolean deletedRecordsSupport = RepositoryConfigurationUtil.isDeletedRecordsEnabled(request);
      final boolean suppressedRecordsSupport = getBooleanProperty(request.getOkapiHeaders(), REPOSITORY_SUPPRESSED_RECORDS_PROCESSING);

      final SourceStorageSourceRecordsClient srsClient = new SourceStorageSourceRecordsClient(request.getOkapiUrl(),
        request.getTenant(), request.getOkapiToken());

      final Date updatedAfter = request.getFrom() == null ? null : convertStringToDate(request.getFrom(), false, true);
      final Date updatedBefore = request.getUntil() == null ? null : convertStringToDate(request.getUntil(), true, true);

      int batchSize = Integer.parseInt(
        RepositoryConfigurationUtil.getProperty(request.getTenant(),
          REPOSITORY_MAX_RECORDS_PER_RESPONSE));

      srsClient.getSourceStorageSourceRecords(
       null,
        null,
        request.getStorageIdentifier(),
        "MARC",
        //1. NULL if we want suppressed and not suppressed, TRUE = ONLY SUPPRESSED FALSE = ONLY NOT SUPPRESSED
        //2. use suppressed from discovery filtering only when deleted record support is enabled
        deletedRecordsSupport ? null : suppressedRecordsSupport,
        deletedRecordsSupport,
        null,
        updatedAfter,
        updatedBefore,
         null,
        request.getOffset(),
        batchSize,
         response -> {
          try {
            if (Response.isSuccess(response.statusCode())) {
              response.bodyHandler(bh -> {
                JsonArray instances = storageHelper.getItems(bh.toJsonObject());
                if (instances != null && !instances.isEmpty()) {
                  promise.complete(retrieveMetadataFormatsWithNoIdentifier(request));
                }else{
                  promise.complete(buildIdentifierNotFoundResponse(request));
                }
              });
            } else {
              logger.error("GetOaiMetadataFormatsHelper response from SRS status code: {}: {}", response.statusMessage(), response.statusCode());
              throw new IllegalStateException(response.statusMessage());
            }

          } catch (Exception e) {
            logger.error("Exception getting list of identifiers", e);
            promise.fail(e);
          }
        });
    } catch (Exception e) {
      logger.error("Error happened while processing ListMetadataFormats verb request", e);
      promise.fail(e);
    }
    return promise.future();
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
   * Builds {@linkplain javax.ws.rs.core.Response Response} with 'badArgument' error because passed identifier is invalid.
   * @return {@linkplain javax.ws.rs.core.Response Response}  with {@link OAIPMH} response
   */
  private javax.ws.rs.core.Response buildBadArgumentResponse(Request request) {
    ResponseHelper responseHelper = getResponseHelper();
    OAIPMH oaipmh = responseHelper.buildOaipmhResponseWithErrors(request, OAIPMHerrorcodeType.BAD_ARGUMENT, INVALID_IDENTIFIER_ERROR_MESSAGE);
    return responseHelper.buildFailureResponse(oaipmh, request);
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
