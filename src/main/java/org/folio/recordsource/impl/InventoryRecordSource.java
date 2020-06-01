package org.folio.recordsource.impl;

import org.folio.oaipmh.Request;
import org.folio.recordsource.AbstractRecordSource;

public class InventoryRecordSource extends AbstractRecordSource {

  public static final String INSTANCES_URI = "/instance-storage/instances";
  public static final String MARC_JSON_RECORD_URI = "/instance-storage/instances/%s/source-record/marc-json";

  private static final String ID = "id";
  public static final String DISCOVERY_SUPPRESS = "discoverySuppress";

  public InventoryRecordSource(Request request) {
    super(request);
  }

  @Override
  public void streamGet() {

  }
}
