package org.folio.rest.impl;

import static org.folio.oaipmh.Constants.INVENTORY_STORAGE;
import static org.folio.oaipmh.Constants.REPOSITORY_STORAGE;

public class OaiPmhInventoryTest extends OaiPmhImplTest {

  @Override
  protected void setStorageType() {
    System.setProperty(REPOSITORY_STORAGE, INVENTORY_STORAGE);
  }
}
