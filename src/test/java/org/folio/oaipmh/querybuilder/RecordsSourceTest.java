package org.folio.oaipmh.querybuilder;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RecordsSourceTest {

  @Test
  void shouldReturnCorrectSourceForSharedRecords() {
    RecordsSource expectedMarc = RecordsSource.CONSORTIUM_MARC;
    RecordsSource expectedFolio = RecordsSource.CONSORTIUM_FOLIO;

    assertEquals(expectedMarc, RecordsSource.getSource("CONSORTIUM-MARC"));
    assertEquals(expectedFolio, RecordsSource.getSource("CONSORTIUM-FOLIO"));
  }
}
