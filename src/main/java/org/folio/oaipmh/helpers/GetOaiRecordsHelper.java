package org.folio.oaipmh.helpers;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.Request;
import org.openarchives.oai._2.ListRecordsType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.RecordType;
import org.openarchives.oai._2.ResumptionTokenType;

import javax.ws.rs.core.Response;
import java.util.Collection;
import java.util.List;

import static org.folio.oaipmh.Constants.INVENTORY;
import static org.folio.oaipmh.Constants.REPOSITORY_MAX_RECORDS_PER_RESPONSE;
import static org.folio.oaipmh.Constants.REPOSITORY_RECORDS_SOURCE;
import static org.folio.oaipmh.Constants.SRS_AND_INVENTORY;
import static org.folio.oaipmh.helpers.RepositoryConfigurationUtil.getProperty;

@Deprecated
public class GetOaiRecordsHelper extends AbstractGetRecordsHelper {

  private static final Logger logger = LogManager.getLogger(GetOaiRecordsHelper.class);

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
        int batchSize = Integer.parseInt(
          RepositoryConfigurationUtil.getProperty(request.getRequestId(),
            REPOSITORY_MAX_RECORDS_PER_RESPONSE));
        requestFromInventory(request, batchSize + 1, request.getIdentifier() != null ? List.of(request.getStorageIdentifier()) : null, false, false)
          .onComplete(handler -> handleInventoryResponse(handler, request, ctx, promise));
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
    return validateListRequest(request);
  }

  @Override
  protected void addRecordsToOaiResponse(OAIPMH oaipmh, Collection<RecordType> records) {
    if (!records.isEmpty()) {
      logger.debug("{} records found for the request.", records.size());
      oaipmh.withListRecords(new ListRecordsType().withRecords(records));
    } else {
      oaipmh.withErrors(createNoRecordsFoundError());
    }
  }

  @Override
  protected void addResumptionTokenToOaiResponse(OAIPMH oaipmh, ResumptionTokenType resumptionToken) {
    if (oaipmh.getListRecords() != null) {
      oaipmh.getListRecords()
            .withResumptionToken(resumptionToken);
    }
  }
}
