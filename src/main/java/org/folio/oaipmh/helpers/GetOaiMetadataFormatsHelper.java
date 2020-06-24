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

      final Date updatedAfter = request.getFrom() == null ? null : convertStringToDate(request.getFrom());
      final Date updatedBefore = request.getUntil() == null ? null : convertStringToDate(request.getUntil());

      int batchSize = Integer.parseInt(
        RepositoryConfigurationUtil.getProperty(request.getTenant(),
          REPOSITORY_MAX_RECORDS_PER_RESPONSE));
      //source-storage/sourceRecords?query=recordType%3D%3DMARC+and+externalIdsHolder.instanceId%3D%3D6eee8eb9-db1a-46e2-a8ad-780f19974efa&limit=51&offset=0
      srsClient.getSourceStorageSourceRecords(
       null,
        null,
        request.getStorageIdentifier(),
        "MARC",
        //NULL if we want suppressed and not suppressed, TRUE = ONLY SUPPRESSED FALSE = ONLY NOT SUPPRESSED
        !suppressedRecordsSupport ? suppressedRecordsSupport : null,
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
//      String endpoint = storageHelper.buildRecordsEndpoint(request, false);
//      getOkapiClient(okapiHeaders)
//        .request(HttpMethod.GET, endpoint, okapiHeaders)
//        .thenApply(response -> verifyAndGetOaiPmhResponse(request, response))
//        .thenAccept(promise::complete)
//        .exceptionally(t -> {
//          promise.fail(t);
//          return null;
//        });
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
   * Validates inventory-mod-storage response and returns {@link OAIPMH} with populated
   * MetadataFormatTypes or needed Errors according to OAI-PMH2 specification
   * @return {@linkplain javax.ws.rs.core.Response Response} with Identifier not found error
   */
  @Deprecated
  private javax.ws.rs.core.Response verifyAndGetOaiPmhResponse(Request request, Response response) {
    if (Response.isSuccess(response.getCode())) {
      JsonArray instances = storageHelper.getItems(response.getBody());
      if (instances != null && !instances.isEmpty()) {
        return retrieveMetadataFormatsWithNoIdentifier(request);
      }
    } else {
      // The storage service could not return an instance so we have to send 500 back to client
      logger.error("No instance found. Service responded with error: " + response.getError());
      throw new IllegalStateException(response.getError().toString());
    }
    return buildIdentifierNotFoundResponse(request);
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
