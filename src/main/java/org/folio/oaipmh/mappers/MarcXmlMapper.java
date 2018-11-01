package org.folio.oaipmh.mappers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import javax.xml.transform.dom.DOMResult;

import org.marc4j.MarcJsonReader;
import org.marc4j.MarcReader;
import org.marc4j.MarcWriter;
import org.marc4j.MarcXmlWriter;
import org.marc4j.marc.DataField;
import org.marc4j.marc.Record;
import org.w3c.dom.Node;

/**
 * Converts MarcJson format to MarcXML format.
 */
public class MarcXmlMapper implements Mapper {

  /**
   * Convert MarcJson to MarcXML.
   *
   * @param source String representation of MarcJson source.
   * @return DOM's Node representation of MarcXML
   */
  public Node convert(String source) {
    try (InputStream inputStream
           = new ByteArrayInputStream(source.getBytes(StandardCharsets.UTF_8))) {
      MarcReader marcJsonReader = new MarcJsonReader(inputStream);
      DOMResult domResult = new DOMResult();
      MarcWriter marcXmlWriter = new MarcXmlWriter(domResult);
      while (marcJsonReader.hasNext()) {
        Record record = marcJsonReader.next();

        /*
         * Fix indicators which comes like "ind1": "\\" in the source string and values are converted to '\'
         * which contradicts to the MARC21slim.xsd schema. So replacing unexpected char by space
         */
        for (DataField data : record.getDataFields()) {
          if (data.getIndicator1() == '\\') {
            data.setIndicator1(' ');
          }
          if (data.getIndicator2() == '\\') {
            data.setIndicator2(' ');
          }
        }
        marcXmlWriter.write(record);
      }
      marcXmlWriter.close();
      return domResult.getNode().getFirstChild().getFirstChild();
    } catch (IOException e) {
      throw new UncheckedIOException(e); //should never happen
    }
  }

}
