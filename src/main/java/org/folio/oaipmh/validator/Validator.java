package org.folio.oaipmh.validator;

import java.util.List;

import org.openarchives.oai._2.OAIPMHerrorType;

import io.vertx.core.Context;

public interface Validator {

  List<OAIPMHerrorType> validate(Object objectToValidate, Context context);

}
