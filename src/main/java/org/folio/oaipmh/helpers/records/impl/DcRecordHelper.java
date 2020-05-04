package org.folio.oaipmh.helpers.records.impl;

import java.util.Collection;
import java.util.List;

import javax.xml.bind.JAXBElement;
import javax.xml.namespace.QName;

import org.folio.oaipmh.helpers.records.RecordHelper;
import org.openarchives.oai._2.RecordType;
import org.openarchives.oai._2_0.oai_dc.Dc;
import org.purl.dc.elements._1.ElementType;

public class DcRecordHelper implements RecordHelper {

  private static final String NAMESPACE_URL = "http://purl.org/dc/elements/1.1/";

  @Override
  public void updateRecordCollectionWithSuppressDiscoveryData(Collection<RecordType> records) {
    records.forEach(record -> {
      boolean isSuppressedFromDiscovery = record.isSuppressDiscovery();
      String suppressDiscoveryValue = isSuppressedFromDiscovery ? "discovery suppressed" : "discovery not suppressed";
      Dc dcRecord = (Dc) record.getMetadata().getAny();
      List<JAXBElement<ElementType>> list = dcRecord.getTitlesAndCreatorsAndSubjects();
      QName name = new QName(NAMESPACE_URL, "rights");
      ElementType elementType = new ElementType().withValue(suppressDiscoveryValue);
      JAXBElement<ElementType> suppressDiscoveryElement = new JAXBElement<>(name, ElementType.class, elementType);
      list.add(suppressDiscoveryElement);
    });
  }
}
