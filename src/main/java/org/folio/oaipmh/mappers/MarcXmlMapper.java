package org.folio.oaipmh.mappers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

import org.marc4j.MarcJsonReader;
import org.marc4j.MarcReader;
import org.marc4j.MarcXmlWriter;
import org.marc4j.marc.DataField;
import org.marc4j.marc.Record;

/**
 * Converts MarcJson format to MarcXML format.
 */
public class MarcXmlMapper implements Mapper {

  /**
   * Convert MarcJson to MarcXML.
   *
   * @param source String representation of MarcJson source.
   * @return byte[] representation of MarcXML
   */
  public byte[] convert(String source) {
    try (InputStream inputStream
           = new ByteArrayInputStream(source.getBytes(StandardCharsets.UTF_8))) {
      MarcReader marcJsonReader = new MarcJsonReader(inputStream);
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      Record record = marcJsonReader.next();

      for (DataField data : record.getDataFields()) {
        if (data.getIndicator1() == '\\') {
          data.setIndicator1(' ');
        }
        if (data.getIndicator2() == '\\') {
          data.setIndicator2(' ');
        }
      }

      MarcXmlWriter.writeSingleRecord(record, out);
      return out.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e); //should never happen
    }
  }

}
