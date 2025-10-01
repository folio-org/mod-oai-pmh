package org.folio.rest.impl;

import java.util.Date;
import lombok.Data;

@Data
public class InventoryUpdatedInstanceIds {
  private String instanceId;
  private String source;
  private Date updatedDate;
  private Boolean suppressFromDiscovery;
  private Boolean deleted;
}
