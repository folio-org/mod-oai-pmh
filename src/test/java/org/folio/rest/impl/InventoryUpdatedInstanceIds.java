
package org.folio.rest.impl;

import lombok.Data;

import java.util.Date;

@Data
public class InventoryUpdatedInstanceIds {
    private String instanceId;
     private String source;
     private Date updatedDate;
     private Boolean suppressFromDiscovery;
     private Boolean deleted;
}
