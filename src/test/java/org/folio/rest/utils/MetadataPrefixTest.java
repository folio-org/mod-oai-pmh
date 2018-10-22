package org.folio.rest.utils;

import org.apache.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.assertSame;

public class MetadataPrefixTest {

  private static final Logger logger = Logger.getLogger(MetadataPrefixTest.class);
  private MetadataPrefix[] metadataPrefixes = MetadataPrefix.values();

  @Test
  public void testGetMarcXmlMapper() {
    logger.info("=== Test get MarcXmlMapper ===");
    for (MetadataPrefix metadataPrefix : metadataPrefixes) {
      assertSame(MetadataPrefix.fromName(metadataPrefix.getName()), metadataPrefix);
    }
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testUnmodifiableFormats() {
    MetadataPrefix.getSupportedMetadataFormats().add("new_format");
  }

}
