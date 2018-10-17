package org.folio.oaipmh;

import org.junit.Test;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.OAIPMHerrorcodeType;
import org.openarchives.oai._2.RequestType;

import javax.xml.bind.JAXBException;
import java.time.Instant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ResponseHelperTest {

  @Test
  public void tests() {
    try {
      ResponseHelper.getInstance().writeToString(new OAIPMH());
      fail("JAXBException is expected because validation is enabled");
    } catch (Exception e) {
      assertTrue("JAXBException expected but was " + e.getMessage(), e instanceof JAXBException);
    }

    try {
      OAIPMH oaipmh = new OAIPMH()
        .withResponseDate(Instant.EPOCH)
        .withRequest(new RequestType().withValue("oai"))
        .withErrors(new OAIPMHerrorType().withCode(OAIPMHerrorcodeType.BAD_VERB).withValue("error"));

      String result = ResponseHelper.getInstance().writeToString(oaipmh);
      assertNotNull(result);

      // Unmarshal string to OAIPMH and verify that these objects equals
      OAIPMH oaipmh1FromString = ResponseHelper.getInstance().stringToOaiPmh(result);
      assertEquals(oaipmh, oaipmh1FromString);
    } catch (JAXBException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }

    try {
      ResponseHelper.getInstance().writeToString(null);
      fail("JAXBException is expected");
    } catch (Exception e) {
      assertTrue("IllegalArgumentException expected but was " + e.getMessage(), e instanceof IllegalArgumentException);
    }
  }
}
