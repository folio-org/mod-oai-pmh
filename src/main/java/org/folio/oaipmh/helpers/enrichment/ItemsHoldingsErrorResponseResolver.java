package org.folio.oaipmh.helpers.enrichment;

import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import java.util.List;
import org.folio.oaipmh.service.ErrorsService;

public class ItemsHoldingsErrorResponseResolver {

  private static final long DELAY_STEP_FOR_ITEM_HOLDING_INVENTORY_REQUEST = 30L;

  private final ItemsHoldingsEnrichment itemsHoldingsEnrichment;
  private final ItemsHoldingsRequestWithDelayExecutor executor;

  public ItemsHoldingsErrorResponseResolver(ItemsHoldingsEnrichment itemsHoldingsEnrichment) {
    this.itemsHoldingsEnrichment = itemsHoldingsEnrichment;
    this.executor = new ItemsHoldingsRequestWithDelayExecutor(itemsHoldingsEnrichment);
  }

  public void processAfterErrors(Promise<List<JsonObject>> enrichInstancesPromise,
      ErrorsService errorsService) {
    long delay = 1L;
    for (String instanceId : itemsHoldingsEnrichment.getInstancesMap().keySet()) {
      executor.execute(delay, instanceId, enrichInstancesPromise, errorsService);
      delay += DELAY_STEP_FOR_ITEM_HOLDING_INVENTORY_REQUEST;
    }
  }
}
