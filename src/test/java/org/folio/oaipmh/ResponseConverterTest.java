package org.folio.oaipmh;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.junit.jupiter.api.Test;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.OAIPMHerrorcodeType;
import org.openarchives.oai._2.RequestType;

import javax.xml.bind.JAXBException;
import java.time.Instant;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.text.IsEmptyString.isEmptyOrNullString;
import static org.junit.jupiter.api.Assertions.fail;


class ResponseConverterTest {

  private static final Logger logger = LoggerFactory.getLogger(ResponseConverterTest.class);

  @Test
  void validationException() {
    ResponseConverter converter = ResponseConverter.getInstance();
    try {
      converter.convertToString(new OAIPMH());
      fail("JAXBException is expected because validation is enabled");
    } catch (IllegalStateException e) {
      assertThat(e.getCause(), instanceOf(JAXBException.class));
    } catch (Exception e) {
      logger.error("Unexpected error", e);
      fail("JAXBException is expected, but was " + e.getMessage());
    }
  }

  @Test
  void validateIllegalArgumentException() {
    ResponseConverter converter = ResponseConverter.getInstance();
    try {
      converter.convertToString(null);
      fail("JAXBException is expected");
    } catch (IllegalArgumentException e) {
      // expected behavior
    } catch (Exception e) {
      logger.error("Unexpected error", e);
      fail("IllegalArgumentException expected but was " + e.getMessage());
    }
  }

  @Test
  void successCase() {
    OAIPMH oaipmh = new OAIPMH()
      .withResponseDate(Instant.EPOCH)
      .withRequest(new RequestType().withValue("oai"))
      .withErrors(new OAIPMHerrorType().withCode(OAIPMHerrorcodeType.BAD_VERB).withValue("error"));

    String result = ResponseConverter.getInstance().convertToString(oaipmh);
    assertThat(result, not(isEmptyOrNullString()));

    // Unmarshal string to OAIPMH and verify that these objects equals
    OAIPMH oaipmhFromString = ResponseConverter.getInstance().stringToOaiPmh(result);

    assertThat(oaipmh, equalTo(oaipmhFromString));
  }
}
