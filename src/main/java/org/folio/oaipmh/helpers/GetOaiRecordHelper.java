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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.MetadataPrefix;
import org.folio.oaipmh.Request;

import org.openarchives.oai._2.GetRecordType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.RecordType;
import org.openarchives.oai._2.ResumptionTokenType;

import javax.ws.rs.core.Response;

public class GetOaiRecordHelper extends AbstractGetRecordsHelper {

  private static final Logger logger = LogManager.getLogger(GetOaiRecordHelper.class);

  @Override
  public Future<Response> handle(Request request, Context ctx) {
    Promise<Response> promise = Promise.promise();
    try {
      List<OAIPMHerrorType> errors = validateRequest(request);
      if (!errors.isEmpty()) {
        return buildResponseWithErrors(request, promise, errors);
      }
      var recordsSource = getProperty(request.getRequestId(), REPOSITORY_RECORDS_SOURCE);
      if (recordsSource.equals(INVENTORY)) {
        requestFromInventory(request, 1, request.getIdentifier() != null ? request.getStorageIdentifier() : null)
          .onComplete(handler -> {
            if (handler.succeeded()) {
              var inventoryRecords = handler.result();
              generateRecordsOnTheFly(request, inventoryRecords);
              processRecords(ctx, request, null, inventoryRecords).onComplete(oaiResponse -> promise.complete(oaiResponse.result()));
            } else {
              logger.error("Request from inventory has been failed due to {}", handler.cause());
              promise.fail(handler.cause());
            }
          });
      } else {
        requestAndProcessSrsRecords(request, ctx, promise, recordsSource.equals(SRS_AND_INVENTORY));
      }
    } catch (Exception e) {
      handleException(promise, e);
    }
    return promise.future();
  }

  @Override
  protected List<OAIPMHerrorType> validateRequest(Request request) {
    List<OAIPMHerrorType> errors = new ArrayList<>();
    if (!validateIdentifier(request)) {
      errors.add(new OAIPMHerrorType().withCode(BAD_ARGUMENT).withValue
        (INVALID_IDENTIFIER_ERROR_MESSAGE));
    }
    if (request.getMetadataPrefix() != null) {
      if (!MetadataPrefix.getAllMetadataFormats().contains(request.getMetadataPrefix())) {
        errors.add(new OAIPMHerrorType().withCode(CANNOT_DISSEMINATE_FORMAT)
          .withValue(CANNOT_DISSEMINATE_FORMAT_ERROR));
      }
    } else {
      errors.add(new OAIPMHerrorType().withCode(BAD_ARGUMENT).withValue
        (RECORD_METADATA_PREFIX_PARAM_ERROR));
    }
    return errors;
  }

  @Override
  protected void addRecordsToOaiResponse(OAIPMH oaipmh, Collection<RecordType> records) {
    if (!records.isEmpty()) {
      oaipmh.withGetRecord(new GetRecordType().withRecord(records.iterator().next()));
    } else {
      oaipmh.withErrors(createNoRecordsFoundError());
    }
  }

  @Override
  protected void addResumptionTokenToOaiResponse(OAIPMH oaipmh, ResumptionTokenType resumptionToken) {
    if (resumptionToken != null) {
      throw new UnsupportedOperationException("Control flow is not applicable for GetRecord verb.");
    }
  }

  @Override
  protected OAIPMHerrorType createNoRecordsFoundError() {
    return new OAIPMHerrorType().withCode(ID_DOES_NOT_EXIST).withValue(RECORD_NOT_FOUND_ERROR);
  }

}
