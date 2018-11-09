package org.folio.oaipmh.helpers;

import org.folio.oaipmh.Request;
import org.folio.oaipmh.ResponseHelper;
import org.openarchives.oai._2.ListRecordsType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.OAIPMHerrorcodeType;
import org.openarchives.oai._2.RecordType;
import org.openarchives.oai._2.ResumptionTokenType;

import java.math.BigInteger;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.folio.rest.jaxrs.resource.Oai.GetOaiRecordsResponse;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_ARGUMENT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_RESUMPTION_TOKEN;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.CANNOT_DISSEMINATE_FORMAT;

public class GetOaiRecordsHelper extends AbstractGetRecordsHelper {

  @Override
  protected List<OAIPMHerrorType> validateRequest(Request request) {
    return validateListRequest(request);
  }

  @Override
  protected void addRecordsToOaiResponse(OAIPMH oaipmh, Collection<RecordType> records) {
    if (!records.isEmpty()) {
      if (logger.isDebugEnabled()) {
        logger.debug(records.size() + " records found for the request.");
      }
      oaipmh.withListRecords(new ListRecordsType().withRecords(records));
    } else {
      oaipmh.withErrors(createNoRecordsFoundError());
    }
  }

  @Override
  protected javax.ws.rs.core.Response buildSuccessResponse(OAIPMH oai) {
    return GetOaiRecordsResponse.respond200WithApplicationXml(ResponseHelper.getInstance().writeToString(oai));
  }

  @Override
  protected javax.ws.rs.core.Response buildResponseWithErrors(OAIPMH oai) {
    String responseBody = ResponseHelper.getInstance().writeToString(oai);

    if (logger.isInfoEnabled()) {
      logger.info("The request processing completed with errors: " + responseBody);
    }

    // According to oai-pmh.raml the service will return different http codes depending on the error
    Set<OAIPMHerrorcodeType> errorCodes = getErrorCodes(oai);
    if (errorCodes.contains(BAD_ARGUMENT) || errorCodes.contains(BAD_RESUMPTION_TOKEN)) {
      return GetOaiRecordsResponse.respond400WithApplicationXml(responseBody);
    } else if (errorCodes.contains(CANNOT_DISSEMINATE_FORMAT)) {
      return GetOaiRecordsResponse.respond422WithApplicationXml(responseBody);
    }
    return GetOaiRecordsResponse.respond404WithApplicationXml(responseBody);
  }

  @Override
  protected void addResumptionTokenToOaiResponse(OAIPMH oaipmh, String resumptionToken, Request request, Integer totalRecords) {
    oaipmh.getListRecords()
      .withResumptionToken(new ResumptionTokenType()
        .withValue(resumptionToken)
        .withCompleteListSize(BigInteger.valueOf(totalRecords))
        .withCursor(request.getOffset() == 0 ? BigInteger.ZERO : BigInteger.valueOf(request.getOffset())));
  }
}
