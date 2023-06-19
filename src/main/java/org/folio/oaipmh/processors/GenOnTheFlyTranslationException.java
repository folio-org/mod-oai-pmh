package org.folio.oaipmh.processors;

import org.folio.processor.error.RecordInfo;

public class GenOnTheFlyTranslationException extends org.folio.processor.error.TranslationException {

  private final String message;

  public GenOnTheFlyTranslationException(RecordInfo recordInfo, Exception exception, String message) {
    super(recordInfo, exception);
    this.message = message;
  }

  @Override
  public String getMessage() {
    return message;
  }

}
