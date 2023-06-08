package org.folio.oaipmh.helpers.enrichment;

import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;

import java.util.List;


public class ItemsHoldingsErrorResponseResolver {

  private static final long DELAY_STEP_FOR_ITEM_HOLDING_INVENTORY_REQUEST = 30L;

  private final ItemsHoldingsEnrichment oaiPmhJsonParserFactory;
  private final boolean isSkipSuppressed;

  public ItemsHoldingsErrorResponseResolver(ItemsHoldingsEnrichment oaiPmhJsonParserFactory,
                                            boolean isSkipSuppressed) {
    this.oaiPmhJsonParserFactory = oaiPmhJsonParserFactory;
    this.isSkipSuppressed = isSkipSuppressed;
  }

  public void processAfterErrors(Promise<List<JsonObject>> enrichInstancesPromise) {
    var executor = new ItemsHoldingsExecutorWithDelay(isSkipSuppressed, oaiPmhJsonParserFactory);
    long delay = 1L;
    for (String instanceId : oaiPmhJsonParserFactory.getInstancesMap().keySet()) {
      executor.execute(delay, instanceId, enrichInstancesPromise);
      delay += DELAY_STEP_FOR_ITEM_HOLDING_INVENTORY_REQUEST;
    }
  }
}
