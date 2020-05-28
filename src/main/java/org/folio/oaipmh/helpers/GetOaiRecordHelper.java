package org.folio.oaipmh.helpers;

import static org.folio.oaipmh.Constants.CANNOT_DISSEMINATE_FORMAT_ERROR;
import static org.folio.oaipmh.Constants.INVALID_IDENTIFIER_ERROR_MESSAGE;
import static org.folio.oaipmh.Constants.RECORD_METADATA_PREFIX_PARAM_ERROR;
import static org.folio.oaipmh.Constants.RECORD_NOT_FOUND_ERROR;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_ARGUMENT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.CANNOT_DISSEMINATE_FORMAT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.ID_DOES_NOT_EXIST;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.folio.oaipmh.MetadataPrefix;
import org.folio.oaipmh.Request;
import org.openarchives.oai._2.GetRecordType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.RecordType;
import org.openarchives.oai._2.ResumptionTokenType;

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
