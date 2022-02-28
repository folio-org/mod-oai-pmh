package org.folio.oaipmh.processors;

import org.apache.commons.lang3.StringUtils;

public class JsonParserErrorResolver {

  private static final String INSTANCE_ID_FIELD = "instanceId";
  private static final String ERROR_COLUMN = "column:";
  private static final String ERROR_MESSAGE_CLOSE_SIGN = "]";
  private static final int ERROR_VALUE_RANGE = 100;

  private final String data;
  private final String localizedMessage;
  private String errorPart;
  private int errorPosition;

  public JsonParserErrorResolver(String data, String localizedMessage) {
    this.data = data;
    this.localizedMessage = localizedMessage;
    initialize();
  }

  public String getErrorPart() {
    return errorPart;
  }

  public int getErrorPosition() {
    return errorPosition;
  }

  private void initialize() {
    int positionFromErrorMessage = getLocalizedMessageErrorPosition();
    int end = Math.min(positionFromErrorMessage + ERROR_VALUE_RANGE, data.length());
    var substring = data.substring(0, end);
    int lastIndexOfInstanceId = substring.lastIndexOf(INSTANCE_ID_FIELD);
    int start = lastIndexOfInstanceId - ERROR_VALUE_RANGE;
    if (start < 0) start = 0;

    errorPart = substring.substring(start);
    errorPosition = positionFromErrorMessage - start;
  }

  private int getLocalizedMessageErrorPosition() {
    var substring =  StringUtils.substringBefore(StringUtils
      .substringAfterLast(localizedMessage, ERROR_COLUMN), ERROR_MESSAGE_CLOSE_SIGN).trim();
    return Integer.parseInt(substring);
  }
}
