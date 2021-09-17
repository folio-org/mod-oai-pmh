package org.folio.oaipmh.helpers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.Request;
import org.openarchives.oai._2.*;

import java.util.Collection;
import java.util.List;

public class GetOaiRecordsHelper extends AbstractGetRecordsHelper {

  private static final Logger logger = LogManager.getLogger(GetOaiRecordsHelper.class);

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
