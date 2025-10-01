package org.folio.oaipmh.mappers;

import static org.folio.oaipmh.service.MetricsCollectingService.MetricOperation.PARSE_XML;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.util.UUID;
import java.util.regex.Pattern;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.service.MetricsCollectingService;
import org.marc4j.MarcJsonReader;
import org.marc4j.MarcReader;
import org.marc4j.MarcXmlWriter;
import org.marc4j.marc.Record;




/**
 * Converts MarcJson format to MarcXML format.
 */
public class MarcXmlMapper implements Mapper {

  private static final Logger logger = LogManager.getLogger(MarcXmlMapper.class);

  private static final Pattern DOUBLE_BACKSLASH_PATTERN = Pattern.compile("\\\\\\\\");

  private MetricsCollectingService metricsCollectingService =
      MetricsCollectingService.getInstance();

  /**
   * Convert MarcJson to MarcXML.
   *
   * @param source String representation of MarcJson source.
   * @return byte[] representation of MarcXML
   */
  public byte[] convert(String source) {

    var operationId = UUID.randomUUID().toString();
    metricsCollectingService.startMetric(operationId, PARSE_XML);

    StopWatch timer = logger.isDebugEnabled() ? StopWatch.createStarted() : null;
    /*
     * Fix indicators which comes like "ind1": "\\" in the source string and values are
     * converted to '\' which contradicts to the MARC21slim.xsd schema. So replacing
     * unexpected char by space
     */
    source = DOUBLE_BACKSLASH_PATTERN.matcher(source).replaceAll(" ");
    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      var stringReader = new StringReader(source);
      MarcReader marcJsonReader = new MarcJsonReader(stringReader);
      Record record = marcJsonReader.next();
      MarcXmlWriter.writeSingleRecord(record, out, false, false);
      return out.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      metricsCollectingService.endMetric(operationId, PARSE_XML);
      if (timer != null) {
        timer.stop();
        logger.debug("Marc-json converted to MarcXml after {} ms.", timer.getTime());
      }
    }
  }
}
