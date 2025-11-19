package org.folio.oaipmh.helpers;

import static org.folio.oaipmh.Constants.CANNOT_DISSEMINATE_FORMAT_ERROR;
import static org.folio.oaipmh.Constants.INVALID_IDENTIFIER_ERROR_MESSAGE;
import static org.folio.oaipmh.Constants.INVENTORY;
import static org.folio.oaipmh.Constants.RECORD_METADATA_PREFIX_PARAM_ERROR;
import static org.folio.oaipmh.Constants.RECORD_NOT_FOUND_ERROR;
import static org.folio.oaipmh.Constants.REPOSITORY_RECORDS_SOURCE;
import static org.folio.oaipmh.Constants.SRS_AND_INVENTORY;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getProperty;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_ARGUMENT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.CANNOT_DISSEMINATE_FORMAT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.ID_DOES_NOT_EXIST;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.ws.rs.core.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.MetadataPrefix;
import org.folio.oaipmh.Request;
import org.openarchives.oai._2.GetRecordType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.RecordType;
import org.openarchives.oai._2.ResumptionTokenType;

public class GetOaiRecordHelper extends AbstractGetRecordsHelper {

  private static final Logger logger = LogManager.getLogger(GetOaiRecordHelper.class);

  @Override
  public Future<Response> handle(Request request, Context ctx) {
    Promise<Response> promise = Promise.promise();
    try {
      logger.info("handle:: Starting GetRecord request - requestId: {}, identifier: {}, "
          + "metadataPrefix: {}", request.getRequestId(), request.getIdentifier(), 
          request.getMetadataPrefix());
      logger.info("handle:: Storage identifier: {} for requestId: {}", 
          request.getStorageIdentifier(), request.getRequestId());
      
      List<OAIPMHerrorType> errors = validateRequest(request);
      if (!errors.isEmpty()) {
        logger.warn("handle:: Validation failed with {} errors for requestId: {}", 
            errors.size(), request.getRequestId());
        for (OAIPMHerrorType error : errors) {
          logger.warn("handle:: Validation error - code: {}, message: {} for requestId: {}", 
              error.getCode(), error.getValue(), request.getRequestId());
        }
        return buildResponseWithErrors(request, promise, errors);
      }
      
      var recordsSource = getProperty(request.getRequestId(), REPOSITORY_RECORDS_SOURCE);
      logger.info("handle:: Records source: '{}' for requestId: {}", 
          recordsSource, request.getRequestId());
      
      if (recordsSource.equals(INVENTORY)) {
        logger.info("handle:: Generate records from inventory by requestId {}",
            request.getRequestId());
        List<String> identifiers = request.getIdentifier() != null
            ? List.of(request.getStorageIdentifier()) : null;
        logger.info("handle:: Requesting from inventory with identifiers: {} for requestId: {}", 
            identifiers, request.getRequestId());
        requestFromInventory(request, 1, identifiers, false, false, true)
            .onComplete(handler -> {
              if (handler.succeeded()) {
                logger.info("handle:: Successfully received for requestId: {}", 
                    request.getRequestId());
              } else {
                logger.error("handle:: Failed to receive response from inventory for requestId: {} "
                    + "with error: {}", request.getRequestId(), 
                    handler.cause() != null ? handler.cause().getMessage() : "unknown");
              }
              handleInventoryResponse(handler, request, ctx, promise);
            });
      } else {
        logger.info("handle:: Process records from srs for requestId {}",
            request.getRequestId());
        requestAndProcessSrsRecords(request, ctx, promise,
            recordsSource.equals(SRS_AND_INVENTORY));
      }
    } catch (Exception e) {
      logger.error("handle:: Request failed for requestId {} with error: {}",
          request.getRequestId(), e.getMessage(), e);
      handleException(promise, e);
    }
    return promise.future();
  }

  @Override
  protected List<OAIPMHerrorType> validateRequest(Request request) {
    logger.info("validateRequest:: Validating request for requestId: {}", request.getRequestId());
    List<OAIPMHerrorType> errors = new ArrayList<>();
    
    boolean identifierValid = validateIdentifier(request);
    logger.info("validateRequest:: Identifier validation result: {} for identifier: '{}' "
        + "(storage: '{}') for requestId: {}", identifierValid, request.getIdentifier(), 
        request.getStorageIdentifier(), request.getRequestId());
    if (!identifierValid) {
      errors.add(new OAIPMHerrorType().withCode(BAD_ARGUMENT)
          .withValue(INVALID_IDENTIFIER_ERROR_MESSAGE));
    }
    
    if (request.getMetadataPrefix() != null) {
      boolean formatSupported = MetadataPrefix.getAllMetadataFormats()
          .contains(request.getMetadataPrefix());
      logger.info("validateRequest:: Metadata prefix validation result: {} for prefix: '{}' "
          + "for requestId: {}", formatSupported, request.getMetadataPrefix(), 
          request.getRequestId());
      if (!formatSupported) {
        errors.add(new OAIPMHerrorType().withCode(CANNOT_DISSEMINATE_FORMAT)
            .withValue(CANNOT_DISSEMINATE_FORMAT_ERROR));
      }
    } else {
      logger.warn("validateRequest:: Metadata prefix is null for requestId: {}", 
          request.getRequestId());
      errors.add(new OAIPMHerrorType().withCode(BAD_ARGUMENT)
          .withValue(RECORD_METADATA_PREFIX_PARAM_ERROR));
    }
    
    logger.info("validateRequest:: Validation completed with {} errors for requestId: {}", 
        errors.size(), request.getRequestId());
    return errors;
  }

  @Override
  protected void addRecordsToOaiResponse(OAIPMH oaipmh, Collection<RecordType> records) {
    logger.info("addRecordsToOaiResponse:: Adding {} records to OAI response", records.size());
    if (!records.isEmpty()) {
      RecordType record = records.iterator().next();
      logger.info("addRecordsToOaiResponse:: Adding record with identifier: {}", 
          record.getHeader() != null ? record.getHeader().getIdentifier() : "null");
      oaipmh.withGetRecord(new GetRecordType().withRecord(record));
    } else {
      logger.warn("addRecordsToOaiResponse:: No records found, adding error response");
      oaipmh.withErrors(createNoRecordFoundError());
    }
  }

  @Override
  protected void addResumptionTokenToOaiResponse(OAIPMH oaipmh,
      ResumptionTokenType resumptionToken) {
    if (resumptionToken != null) {
      throw new UnsupportedOperationException(
          "Control flow is not applicable for GetRecord verb.");
    }
  }

  @Override
  public Response buildNoRecordsFoundOaiResponse(OAIPMH oaipmh, Request request) {
    logger.warn("buildNoRecordsFoundOaiResponse:: Building no records found response for "
        + "identifier: '{}' for requestId: {}", request.getIdentifier(), request.getRequestId());
    oaipmh.withErrors(createNoRecordFoundError());
    return getResponseHelper().buildFailureResponse(oaipmh, request);
  }

  private OAIPMHerrorType createNoRecordFoundError() {
    logger.info("createNoRecordFoundError:: Creating ID_DOES_NOT_EXIST error");
    return new OAIPMHerrorType().withCode(ID_DOES_NOT_EXIST).withValue(RECORD_NOT_FOUND_ERROR);
  }

}
