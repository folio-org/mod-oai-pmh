package org.folio.oaipmh.service;

import io.vertx.core.Context;
import io.vertx.core.Future;
import org.folio.dataimport.util.OkapiConnectionParams;

public interface IndetifiersCleanUpService{

  Future<Boolean> cleanUp(OkapiConnectionParams params, Context vertxContext);

}
