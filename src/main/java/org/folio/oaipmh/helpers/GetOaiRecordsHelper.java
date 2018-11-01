package org.folio.oaipmh.helpers;

import org.folio.oaipmh.Request;
import org.folio.oaipmh.ResponseHelper;
import org.openarchives.oai._2.ListRecordsType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.OAIPMHerrorcodeType;
import org.openarchives.oai._2.RecordType;
import org.openarchives.oai._2.RequestType;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.folio.rest.jaxrs.resource.Oai.GetOaiRecordsResponse;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_ARGUMENT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_RESUMPTION_TOKEN;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.CANNOT_DISSEMINATE_FORMAT;
import static org.openarchives.oai._2.VerbType.LIST_RECORDS;

public class GetOaiRecordsHelper extends AbstractGetRecordsHelper {

  @Override
  protected List<OAIPMHerrorType> validateRequest(Request request) {
    return validateListRequest(request);
  }

  @Override
  protected OAIPMH addRecordsToOaiResponce(OAIPMH oaipmh, Collection<RecordType> records) {
    return oaipmh.withListRecords(new ListRecordsType().withRecords(records));
  }

  @Override
  protected javax.ws.rs.core.Response buildSuccessResponse(OAIPMH oai) {
    return GetOaiRecordsResponse.respond200WithApplicationXml(ResponseHelper.getInstance().writeToString(oai));
  }

  @Override
  protected javax.ws.rs.core.Response buildResponseWithErrors(OAIPMH oai) {
    String responseBody = ResponseHelper.getInstance().writeToString(oai);

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
  protected OAIPMH buildBaseResponse(RequestType request) {
    return super.buildBaseResponse(request.withVerb(LIST_RECORDS));
  }
}
