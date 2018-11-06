package org.folio.oaipmh.mappers;

import org.marc4j.MarcJsonReader;
import org.marc4j.MarcReader;
import org.marc4j.MarcXmlWriter;
import org.marc4j.marc.Record;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

/**
 * Converts MarcJson format to MarcXML format.
 */
public class MarcXmlMapper implements Mapper {

  private static final Pattern DOUBLE_BACKSLASH_PATTERN = Pattern.compile("\\\\\\\\");

  /**
   * Convert MarcJson to MarcXML.
   *
   * @param source String representation of MarcJson source.
   * @return byte[] representation of MarcXML
   */
  public byte[] convert(String source) {
    /*
     * Fix indicators which comes like "ind1": "\\" in the source string and values are converted to '\'
     * which contradicts to the MARC21slim.xsd schema. So replacing unexpected char by space
     */
    source = DOUBLE_BACKSLASH_PATTERN.matcher(source).replaceAll(" ");
    try (InputStream inputStream
           = new ByteArrayInputStream(source.getBytes(StandardCharsets.UTF_8))) {
      MarcReader marcJsonReader = new MarcJsonReader(inputStream);
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      Record record = marcJsonReader.next();
      MarcXmlWriter.writeSingleRecord(record, out);
      return out.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e); //should never happen
    }
  }

}
