package org.folio.oaipmh.querybuilder;

public enum RecordsSource {
  FOLIO, MARC, FOLIO_SHARED, MARC_SHARED, CONSORTIUM_MARC, CONSORTIUM_FOLIO, LINKED_DATA;

  public static RecordsSource getSource(String name) {
    if (name.equals("Inventory")) {
      return FOLIO;
    } else if (name.equals("Source record storage")) {
      return MARC;
    } else if (name.equals("Source record storage and Inventory") ||
      name.equals("Source records storage and Inventory")) {
      return null; // SRS + Inventory means no need to specify source in SQL.
    } else if (name.equals("CONSORTIUM-MARC")) {
      return CONSORTIUM_MARC;
    } else if (name.equals("CONSORTIUM-FOLIO")) {
      return CONSORTIUM_FOLIO;
    } else if (name.equals("LINKED_DATA")) {
      return LINKED_DATA;
    }
    return valueOf(name);
  }

  @Override
  public String toString() {
    var res = super.toString();
    if (res.equals("CONSORTIUM_MARC")) {
      return "CONSORTIUM-MARC";
    } else if (res.equals("CONSORTIUM_FOLIO")) {
      return "CONSORTIUM-FOLIO";
    }
    return res;
  }
}
