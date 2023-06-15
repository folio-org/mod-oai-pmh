package org.folio.oaipmh.processors;

import org.folio.processor.error.RecordInfo;

public class TranslationException extends org.folio.processor.error.TranslationException {

  private String message;

  public TranslationException(RecordInfo recordInfo, Exception exception, String message) {
    super(recordInfo, exception);
    this.message = message;
  }

  @Override
  public String getMessage() {
    return message;
  }

}
