package org.folio.oaipmh.querybuilder;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class RecordsSourceTest {

  @Test
  void shouldReturnCorrectSourceForSharedRecords() {
    RecordsSource expectedMarc = RecordsSource.CONSORTIUM_MARC;
    RecordsSource expectedFolio = RecordsSource.CONSORTIUM_FOLIO;

    assertEquals(expectedMarc, RecordsSource.getSource("CONSORTIUM-MARC"));
    assertEquals(expectedFolio, RecordsSource.getSource("CONSORTIUM-FOLIO"));
  }

  @Test
  void shouldReturnCorrectSourceForLinkedData() {
    RecordsSource expectedSource = RecordsSource.LINKED_DATA;
    assertEquals(expectedSource, RecordsSource.getSource("LINKED_DATA"));

    expectedSource = RecordsSource.CONSORTIUM_LINKED_DATA;
    assertEquals(expectedSource, RecordsSource.getSource("CONSORTIUM-LINKED_DATA"));
  }
}
