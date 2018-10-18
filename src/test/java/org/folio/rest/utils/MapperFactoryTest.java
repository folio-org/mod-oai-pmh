package org.folio.rest.utils;

import org.apache.log4j.Logger;
import org.folio.oaipmh.MetadataPrefix;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class MapperFactoryTest {

  private static final Logger logger = Logger.getLogger(MarcJsonToMarcXmlMapperTest.class);

  private MapperFactory mapperFactory;

  @Before
  public void setUp() {
    mapperFactory = new MapperFactory();
  }

  @Test
  public void testGetMarcXmlMapper() {
    logger.info("=== Test get MarcXmlMapper ===");
    Mapper mapper = mapperFactory.getMapper(MetadataPrefix.MARC_XML);
    assertThat(mapper, instanceOf(MarcXmlMapper.class));
  }

  @Test
  public void testGetXSLTMapper() {
    logger.info("=== Test get XSLTMapper ===");
    Mapper mapper = mapperFactory.getMapper(MetadataPrefix.DC);
    assertThat(mapper, instanceOf(XSLTMapper.class));
  }
}
