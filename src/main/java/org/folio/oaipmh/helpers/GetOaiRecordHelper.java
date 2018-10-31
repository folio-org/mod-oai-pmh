package org.folio.oaipmh.helpers;

import org.folio.oaipmh.MetadataPrefix;
import org.folio.oaipmh.Request;
import org.openarchives.oai._2.GetRecordType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.RecordType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.folio.oaipmh.Constants.CANNOT_DISSEMINATE_FORMAT_ERROR;
import static org.folio.oaipmh.Constants.RECORD_NO_REQUIRED_PARAM_ERROR;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.BAD_ARGUMENT;
import static org.openarchives.oai._2.OAIPMHerrorcodeType.CANNOT_DISSEMINATE_FORMAT;

public class GetOaiRecordHelper extends AbstractGetRecordsHelper {

  @Override
  protected OAIPMH addRecordsToOaiResponce(OAIPMH oaipmh, Collection<RecordType> records) {
    return oaipmh.withGetRecord(new GetRecordType().withRecord(records.iterator().next()));
  }

  @Override
  public List<OAIPMHerrorType> validateRequest(Request request) {
    List<OAIPMHerrorType> errors = new ArrayList<>();

    if (request.getMetadataPrefix() != null) {
      if (!MetadataPrefix.getAllMetadataFormats().contains(request.getMetadataPrefix())) {
        errors.add(new OAIPMHerrorType().withCode(CANNOT_DISSEMINATE_FORMAT)
          .withValue(String.format(CANNOT_DISSEMINATE_FORMAT_ERROR, request.getMetadataPrefix())));
      }
    } else {
      errors.add(new OAIPMHerrorType().withCode(BAD_ARGUMENT).withValue
        (RECORD_NO_REQUIRED_PARAM_ERROR));
    }
    return errors;
  }
}
