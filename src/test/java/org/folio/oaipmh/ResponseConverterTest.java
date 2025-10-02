package org.folio.oaipmh;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.text.IsEmptyString.isEmptyOrNullString;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Instant;
import javax.xml.bind.JAXBException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorType;
import org.openarchives.oai._2.OAIPMHerrorcodeType;
import org.openarchives.oai._2.RequestType;

class ResponseConverterTest {

  private static final Logger logger = LogManager.getLogger(ResponseConverterTest.class);

  @Test
  void validationException() {
    ResponseConverter converter = ResponseConverter.getInstance();
    OAIPMH oaipmh = new OAIPMH();
    try {
      converter.convertToString(oaipmh);
      fail("JAXBException is expected because validation is enabled");
    } catch (IllegalStateException e) {
      assertThat(e.getCause(), instanceOf(JAXBException.class));
    } catch (Exception e) {
      logger.error("Unexpected error.", e);
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
      logger.error("Unexpected error.", e);
      fail("IllegalArgumentException expected but was " + e.getMessage());
    }
  }

  @Test
  void successCase() {
    OAIPMH oaipmh = new OAIPMH()
        .withResponseDate(Instant.EPOCH)
        .withRequest(new RequestType().withValue("oai"))
        .withErrors(new OAIPMHerrorType()
        .withCode(OAIPMHerrorcodeType.BAD_VERB)
        .withValue("error"));

    String result = ResponseConverter.getInstance().convertToString(oaipmh);
    assertThat(result, not(isEmptyOrNullString()));

    // Unmarshal string to OAIPMH and verify that these objects equals
    OAIPMH oaipmhFromString = ResponseConverter.getInstance().stringToOaiPmh(result);

    assertThat(oaipmh, equalTo(oaipmhFromString));
  }

  @Test
  void shouldProvideDetailedErrorMessage() {
    var source = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><marc:record xmlns:marc="
        + "\"http://www.loc.gov/MARC21/slim\"><marc:leader>01344nja a2200289 c 4500</"
        + "marc:leader><marc:datafield tag=\"035\" ind1=\" \" ind2=\" \"><marc:subfield"
        + " code=\"a\">(DE-599)GBV\u001f1011162431</marc:subfield></marc:record>\n";
    var converter = ResponseConverter.getInstance();
    var exception = assertThrows(IllegalStateException.class, () ->
        converter.bytesToObject(source.getBytes()));
    assertThat(exception.getMessage(), containsString("An invalid XML character (Unicode: 0x1f)"
        + " was found in the element content of the document."));
  }
}
