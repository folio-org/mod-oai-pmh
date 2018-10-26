package org.folio.oaipmh;

import org.apache.log4j.Logger;
import org.junit.jupiter.api.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MetadataPrefixTest {

  private static final Logger logger = Logger.getLogger(MetadataPrefixTest.class);
  private MetadataPrefix[] metadataPrefixes = MetadataPrefix.values();

  @Test
  void testGetMetadataPrefixFromName() {
    logger.info("=== Test get metadata prefix from name ===");
    for (MetadataPrefix metadataPrefix : metadataPrefixes) {
      assertThat(MetadataPrefix.fromName(metadataPrefix.getName()), is(metadataPrefix));
    }
  }

  @Test
  void testUnmodifiableFormats() {
    logger.info("=== Test if Set of metadata prefixes is unmodifiable ===");
    assertThrows(UnsupportedOperationException.class, () -> MetadataPrefix.getAllMetadataFormats().add("new_format"));
  }

}
