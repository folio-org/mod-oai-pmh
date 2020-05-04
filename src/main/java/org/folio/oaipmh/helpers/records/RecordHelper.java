package org.folio.oaipmh.helpers.records;

import java.util.Collection;

import org.openarchives.oai._2.RecordType;

public interface RecordHelper {

  void updateRecordCollectionWithSuppressDiscoveryData(Collection<RecordType> records);

}
