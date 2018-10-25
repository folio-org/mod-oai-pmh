package org.folio.oaipmh;

import org.apache.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.assertSame;

public class MetadataPrefixTest {

  private static final Logger logger = Logger.getLogger(MetadataPrefixTest.class);
  private MetadataPrefix[] metadataPrefixes = MetadataPrefix.values();

  @Test
  public void testGetMetadataPrefixFromName() {
    logger.info("=== Test get metadata prefix from name ===");
    for (MetadataPrefix metadataPrefix : metadataPrefixes) {
      assertSame(MetadataPrefix.fromName(metadataPrefix.getName()), metadataPrefix);
    }
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testUnmodifiableFormats() {
    logger.info("=== Test if Set of metadata prefixes is unmodifiable ===");
    MetadataPrefix.getAllMetadataFormats().add("new_format");
  }

}
