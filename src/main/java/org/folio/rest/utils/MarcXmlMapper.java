package org.folio.rest.utils;

import org.marc4j.MarcJsonReader;
import org.marc4j.MarcReader;
import org.marc4j.MarcWriter;
import org.marc4j.MarcXmlWriter;
import org.marc4j.marc.Record;
import org.w3c.dom.Node;

import javax.xml.transform.dom.DOMResult;
import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Converts MarkJson format to MarcXML format.
 */
public class MarcXmlMapper implements Mapper {

  /**
   * {@inheritDoc}
   * @param source String representation of MarkJson source.
   * @return the Node object that represents result MarcXML document
   */
  @Override
  public Node convert(String source) {
    try(InputStream inputStream
          = new ByteArrayInputStream(source.getBytes(StandardCharsets.UTF_8))) {
      MarcReader marcJsonReader = new MarcJsonReader(inputStream);
      DOMResult domResult = new DOMResult();
      MarcWriter marcXmlWriter = new MarcXmlWriter(domResult);
      while (marcJsonReader.hasNext()) {
        Record record = marcJsonReader.next();
        marcXmlWriter.write(record);
      }
      marcXmlWriter.close();
      return domResult.getNode();
    } catch (IOException e) {
      throw new UncheckedIOException(e); //should never happen
    }

  }

}
