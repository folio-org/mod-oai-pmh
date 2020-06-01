package org.folio.recordsource;

import org.folio.oaipmh.Request;
import org.folio.recordsource.impl.InventoryAndSRSRecordSource;

public abstract class RecordSourceStrategy{

  public static RecordSource byDefault(Request request){
    return new InventoryAndSRSRecordSource(request);
  };

  public static RecordSource legacy(){
    return null;
  };

}
