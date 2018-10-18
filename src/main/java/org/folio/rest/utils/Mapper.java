package org.folio.rest.utils;

import org.w3c.dom.Node;

import java.io.IOException;

public interface Mapper {
  Node convert(String source) throws IOException;
}
