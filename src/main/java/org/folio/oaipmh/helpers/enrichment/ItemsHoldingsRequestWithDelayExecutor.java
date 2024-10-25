package org.folio.oaipmh.helpers.enrichment;

import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.codec.BodyCodec;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import static org.folio.oaipmh.Constants.SKIP_SUPPRESSED_FROM_DISCOVERY_RECORDS;
import static org.folio.oaipmh.helpers.AbstractGetRecordsHelper.INSTANCE_IDS_ENRICH_PARAM_NAME;
import org.folio.oaipmh.processors.OaiPmhJsonParser;
import org.folio.oaipmh.service.ErrorsService;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ItemsHoldingsRequestWithDelayExecutor {

  private static final Logger logger = LogManager.getLogger(ItemsHoldingsRequestWithDelayExecutor.class);

  private final Vertx vertx = Vertx.currentContext().owner();
  private final ItemsHoldingsEnrichment itemsHoldingsEnrichment;
  private final AtomicInteger inc = new AtomicInteger();
  private final int size;

  public ItemsHoldingsRequestWithDelayExecutor(ItemsHoldingsEnrichment itemsHoldingsEnrichment) {
    this.itemsHoldingsEnrichment = itemsHoldingsEnrichment;
    this.size = itemsHoldingsEnrichment.getInstancesMap().size() - 1;
  }

  public void execute(long delay, String instanceId, Promise<List<JsonObject>> enrichInstancesPromise, ErrorsService errorsService) {
    vertx.setTimer(delay, id -> {
      var httpRequest = ItemsHoldingInventoryRequestFactory.getItemsHoldingsInventoryRequest(itemsHoldingsEnrichment.getRequest());
      JsonObject entries = new JsonObject();
      entries.put(INSTANCE_IDS_ENRICH_PARAM_NAME, new JsonArray(List.of(instanceId)));
      entries.put(SKIP_SUPPRESSED_FROM_DISCOVERY_RECORDS, itemsHoldingsEnrichment.isSkipSuppressed());
      var jsonParser = itemsHoldingsEnrichment.getJsonParser();
      httpRequest.as(BodyCodec.jsonStream(jsonParser))
        .sendBuffer(entries.toBuffer())
        .onSuccess(response -> {
          if (response.statusCode() != 200) {
            var errors = ((OaiPmhJsonParser)jsonParser).getErrors();
            errors.forEach(error -> {
              errorsService.log(itemsHoldingsEnrichment.getRequest().getTenant(), itemsHoldingsEnrichment.getRequest().getRequestId(),
                instanceId, error);
              logger.error("Error for requestId {} and instanceId {}  with message {}",
                itemsHoldingsEnrichment.getRequest().getRequestId(), instanceId, error);
            });
          }
          if (inc.get() < size) {
            inc.incrementAndGet();
          } else {
            enrichInstancesPromise.complete(new ArrayList<>(itemsHoldingsEnrichment.getInstancesMap().values()));
          }
        }).onFailure(e -> enrichInstancesPromise.fail(e.getMessage()));
    });
  }
}
