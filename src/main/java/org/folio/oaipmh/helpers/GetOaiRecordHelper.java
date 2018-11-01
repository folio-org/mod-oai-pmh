package org.folio.oaipmh.helpers;

import org.folio.oaipmh.MetadataPrefix;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.ResponseHelper;

import org.openarchives.oai._2.GetRecordType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.OAIPMHerrorcodeType;
import org.openarchives.oai._2.RecordType;

import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.folio.oaipmh.Constants.*;
import static org.folio.rest.jaxrs.resource.Oai.GetOaiRecordsByIdResponse.respond200WithApplicationXml;
import static org.folio.rest.jaxrs.resource.Oai.GetOaiRecordsByIdResponse.respond404WithApplicationXml;
import static org.folio.rest.jaxrs.resource.Oai.GetOaiRecordsByIdResponse.respond422WithApplicationXml;
import static org.folio.rest.jaxrs.resource.Oai.GetOaiRecordsByIdResponse.respond400WithApplicationXml;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.*;

public class GetOaiRecordHelper extends AbstractGetRecordsHelper {

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
  protected void addRecordsToOaiResponce(OAIPMH oaipmh, Collection<RecordType> records) {
    oaipmh.withGetRecord(new GetRecordType().withRecord(records.iterator().next()));
  }

  @Override
  protected Response buildResponseWithErrors(OAIPMH oai) {
    String responseBody = ResponseHelper.getInstance().writeToString(oai);

    // According to oai-pmh.raml the service will return different http codes depending on the error
    Set<OAIPMHerrorcodeType> errorCodes = getErrorCodes(oai);
    if (errorCodes.contains(BAD_ARGUMENT)) {
      return respond400WithApplicationXml(responseBody);
    } else if (errorCodes.contains(CANNOT_DISSEMINATE_FORMAT)) {
      return respond422WithApplicationXml(responseBody);
    }
    return respond404WithApplicationXml(responseBody);
  }

  @Override
  protected Response buildSuccessResponse(OAIPMH oai) {
    return respond200WithApplicationXml(ResponseHelper.getInstance().writeToString(oai));
  }

  @Override
  protected OAIPMHerrorType createNoRecordsFoundError() {
    return new OAIPMHerrorType().withCode(ID_DOES_NOT_EXIST).withValue(RECORD_NOT_FOUND_ERROR);
  }

}
