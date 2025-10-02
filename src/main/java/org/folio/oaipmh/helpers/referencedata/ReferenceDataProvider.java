package org.folio.oaipmh.helpers.referencedata;

import static org.folio.processor.referencedata.ReferenceDataConstants.ALTERNATIVE_TITLE_TYPES;
import static org.folio.processor.referencedata.ReferenceDataConstants.CALL_NUMBER_TYPES;
import static org.folio.processor.referencedata.ReferenceDataConstants.CAMPUSES;
import static org.folio.processor.referencedata.ReferenceDataConstants.CONTRIBUTOR_NAME_TYPES;
import static org.folio.processor.referencedata.ReferenceDataConstants.ELECTRONIC_ACCESS_RELATIONSHIPS;
import static org.folio.processor.referencedata.ReferenceDataConstants.IDENTIFIER_TYPES;
import static org.folio.processor.referencedata.ReferenceDataConstants.INSTANCE_FORMATS;
import static org.folio.processor.referencedata.ReferenceDataConstants.INSTANCE_TYPES;
import static org.folio.processor.referencedata.ReferenceDataConstants.INSTITUTIONS;
import static org.folio.processor.referencedata.ReferenceDataConstants.LIBRARIES;
import static org.folio.processor.referencedata.ReferenceDataConstants.LOAN_TYPES;
import static org.folio.processor.referencedata.ReferenceDataConstants.LOCATIONS;
import static org.folio.processor.referencedata.ReferenceDataConstants.MATERIAL_TYPES;

import org.folio.oaipmh.Request;
import org.folio.oaipmh.helpers.client.InventoryClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ReferenceDataProvider {
  private static final int CACHE_EXPIRATION_AFTER_ACCESS_SECONDS = 60;

  private static final String ISSUANCE_MODES = "issuanceModes";
  private static final String HOLDING_NOTE_TYPES = "holdingsNoteTypes";
  private static final String ITEM_NOTE_TYPES = "itemNoteTypes";
  private static final String CONTENT_TERMS = "natureOfContentTerms";
  private Cache<String, ReferenceData> cache;
  private InventoryClient inventoryClient;

  public ReferenceDataProvider(@Autowired InventoryClient inventoryClient) {
    this.inventoryClient = inventoryClient;
    this.cache = new Cache<>(CACHE_EXPIRATION_AFTER_ACCESS_SECONDS);
  }

  public ReferenceData get(Request request) {
    ReferenceData cached = this.cache.get(request.getRequestId());
    if (cached == null) {
      ReferenceData loaded = load(request);
      this.cache.put(request.getRequestId(), loaded);
      return loaded;
    } else {
      return cached;
    }
  }

  /**
   * These methods returns the reference data that is needed to map the fields to MARC,
   * while generating marc records on the fly.
   */
  private ReferenceDataImpl load(Request request) {
    ReferenceDataImpl referenceData = new ReferenceDataImpl();
    referenceData.put(ALTERNATIVE_TITLE_TYPES, inventoryClient.getAlternativeTitleTypes(request));
    referenceData.put(CONTENT_TERMS, inventoryClient.getNatureOfContentTerms(request));
    referenceData.put(IDENTIFIER_TYPES, inventoryClient.getIdentifierTypes(request));
    referenceData.put(CONTRIBUTOR_NAME_TYPES, inventoryClient.getContributorNameTypes(request));
    referenceData.put(LOCATIONS, inventoryClient.getLocations(request));
    referenceData.put(LOAN_TYPES, inventoryClient.getLoanTypes(request));
    referenceData.put(LIBRARIES, inventoryClient.getLibraries(request));
    referenceData.put(CAMPUSES, inventoryClient.getCampuses(request));
    referenceData.put(INSTITUTIONS, inventoryClient.getInstitutions(request));
    referenceData.put(MATERIAL_TYPES, inventoryClient.getMaterialTypes(request));
    referenceData.put(INSTANCE_TYPES, inventoryClient.getInstanceTypes(request));
    referenceData.put(INSTANCE_FORMATS, inventoryClient.getInstanceFormats(request));
    referenceData.put(ELECTRONIC_ACCESS_RELATIONSHIPS,
        inventoryClient.getElectronicAccessRelationships(request));
    referenceData.put(ISSUANCE_MODES, inventoryClient.getModesOfIssuance(request));
    referenceData.put(CALL_NUMBER_TYPES, inventoryClient.getCallNumberTypes(request));
    referenceData.put(ITEM_NOTE_TYPES, inventoryClient.getItemNoteTypes(request));
    referenceData.put(HOLDING_NOTE_TYPES, inventoryClient.getHoldingsNoteTypes(request));
    return referenceData;
  }
}
