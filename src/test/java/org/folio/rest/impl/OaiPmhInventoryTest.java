package org.folio.rest.impl;

import static org.folio.oaipmh.Constants.INVENTORY_STORAGE;
import static org.folio.oaipmh.Constants.REPOSITORY_STORAGE;

import org.folio.oaipmh.MetadataPrefix;

public class OaiPmhInventoryTest extends OaiPmhImplTest {

  @Override
  protected void setStorageType() {
    System.setProperty(REPOSITORY_STORAGE, INVENTORY_STORAGE);
  }

  /**
   * Since externalIdsHolder field relates only for SRS sourceRecords, such test for inventory instances doesn't have any matter and
   * cannot be reproduced.
   *
   * @param metadataPrefix - {@link MetadataPrefix}
   * @param encoding       - encoding
   */
  @Override
  void getOaiListRecordsVerbWithOneWithoutExternalIdsHolderField(MetadataPrefix metadataPrefix, String encoding) {
  }

  /**
   * Since externalIdsHolder field relates only for SRS sourceRecords, such test for inventory instances doesn't have any matter and
   * cannot be reproduced.
   *
   * @param metadataPrefix - {@link MetadataPrefix}
   * @param encoding       - encoding
   */
  @Override
  void getOaiIdentifiersVerbOneRecordWithoutExternalIdsHolderField(MetadataPrefix metadataPrefix, String encoding) {

  }
}
