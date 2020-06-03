package org.folio.recordsource.impl;

import javax.ws.rs.core.Response;

import org.folio.oaipmh.Request;
import org.folio.recordsource.AbstractRecordSource;

import io.vertx.core.Future;

public class InventoryAndSRSRecordSource extends AbstractRecordSource {

  public static final String INSTANCES_URI = "/instance-storage/instances";
  public static final String MARC_JSON_RECORD_URI = "/instance-storage/instances/%s/source-record/marc-json";

  private static final String ID = "id";
  public static final String DISCOVERY_SUPPRESS = "discoverySuppress";

  public InventoryAndSRSRecordSource(Request request) {
    super(request);
  }


  @Override
  public Future<Response> getAll() {
//    SourceStorageClient ssc = new SourceStorageClient(request.getOkapiUrl(), request.getTenant(), request.getOkapiToken());



    return null;
  }

  @Override
  public void streamGet() {

  }
}
