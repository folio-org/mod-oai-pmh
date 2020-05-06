package org.folio.oaipmh.helpers.records;

import static java.lang.String.format;

import java.util.Collection;

import org.folio.oaipmh.MetadataPrefix;
import org.folio.oaipmh.helpers.records.impl.DcRecordHelper;
import org.folio.oaipmh.helpers.records.impl.MarcRecordHelper;
import org.openarchives.oai._2.RecordType;

/**
 * Record helpers is used for manipulating under the record collection that composes the oai-pmh feed.
 * The main operations for which it is used are adding,deleting and formatting record fields and etc.
 */
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

  /**
   * Updates records with 'record discovery suppression state' data.
   *
   * @param records - records to be updated
   */
  void updateRecordCollectionWithSuppressDiscoveryData(Collection<RecordType> records);

}
