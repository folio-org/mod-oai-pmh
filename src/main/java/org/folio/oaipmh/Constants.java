package org.folio.oaipmh;

public final class Constants {

  public static final String REPOSITORY_BASE_URL = "repository.baseURL";
  public static final String IDENTIFIER_PREFIX = "identifierPrefix";

  public static final String FROM_PARAM = "from";
  public static final String IDENTIFIER_PARAM = "identifier";
  public static final String METADATA_PREFIX_PARAM = "metadataPrefix";
  public static final String RESUMPTION_TOKEN_PARAM = "resumptionToken";
  public static final String SET_PARAM = "set";
  public static final String UNTIL_PARAM = "until";

  public static final String CANNOT_DISSEMINATE_FORMAT_ERROR = "The value '%s' of the metadataPrefix argument is not supported by the repository";
  public static final String RESUMPTION_TOKEN_FORMAT_ERROR = "The value '%s' of the resumptionToken argument is invalid";
  public static final String LIST_NO_REQUIRED_PARAM_ERROR = "The request is missing required arguments. There is no metadataPrefix nor resumptionToken";
  public static final String LIST_ILLEGAL_ARGUMENTS_ERROR = "The request includes resumptionToken and other argument(s)";
  public static final String NO_RECORD_FOUND_ERROR = "There is no any record found matching search criteria";
  public static final String BAD_DATESTAMP_FORMAT_ERROR = "Bad datestamp format for '%s=%s' argument.";
  public static final String RECORD_NO_REQUIRED_PARAM_ERROR ="The request is missing required arguments. There is no metadataPrefix.";
  public static final String RECORD_NOT_FOUND_ERROR = "No matching identifier in repository.";
}
