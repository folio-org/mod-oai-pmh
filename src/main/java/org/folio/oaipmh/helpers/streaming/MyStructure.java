package org.folio.oaipmh.helpers.streaming;

import java.util.Objects;

//TODO JAVADOC OR REMOVE
public class MyStructure {

  public final String id;
  public final BatchStreamWrapper stream;

  public MyStructure(String id, BatchStreamWrapper stream) {
    this.id = id;
    this.stream = stream;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    MyStructure that = (MyStructure) o;
    return Objects.equals(id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }
}
