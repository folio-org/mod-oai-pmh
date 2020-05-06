package org.folio.oaipmh.helpers.records;

import static java.lang.String.format;

import java.util.Collection;

import org.folio.oaipmh.MetadataPrefix;
import org.folio.oaipmh.helpers.records.impl.DcRecordHelper;
import org.folio.oaipmh.helpers.records.impl.MarcRecordHelper;
import org.openarchives.oai._2.RecordType;

public interface RecordHelper {

  /**
   * Creates instance of the StorageHelper depending on the metadata prefix.
   */
  static RecordHelper getInstance(MetadataPrefix metadataPrefix) {
    switch(metadataPrefix) {
      case MARC21XML: return new MarcRecordHelper();
      case DC: return new DcRecordHelper();
      default: throw new UnsupportedOperationException(format("Metadata prefix \"%s\" is not supported.",metadataPrefix.getName()));
    }
  }

  void updateRecordCollectionWithSuppressDiscoveryData(Collection<RecordType> records);

}
