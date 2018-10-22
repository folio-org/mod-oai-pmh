package org.folio.oaipmh;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * Class that represents OAI-PMH request and holds http query arguments.
 * It implements builder pattern, so use {@link Builder} instance to build an instance of the request.
 */
public class Request {
  private String metadataPrefix;
  private String identifier;
  private String from;
  private String until;
  private String set;
  private String resumptionToken;
  private Map<String, String> okapiHeaders;

  /**
   * Builder used to build the request.
   */
  public static class Builder {
    private String metadataPrefix;
    private String identifier;
    private String from;
    private String until;
    private String set;
    private String resumptionToken;
    private Map<String, String> okapiHeaders;

    public Builder metadataPrefix(String metadataPrefix) {
      this.metadataPrefix = metadataPrefix;
      return this;
    }

    public Builder identifier(String identifier) {
      this.identifier = identifier;
      return this;
    }

    public Builder from(String from) {
      this.from = from;
      return this;
    }

    public Builder until(String until) {
      this.until = until;
      return this;
    }

    public Builder set(String set) {
      this.set = set;
      return this;
    }

    public Builder resumptionToken(String resumptionToken) {
      this.resumptionToken = resumptionToken;
      return this;
    }

    public Builder okapiHeaders(Map<String, String> okapiHeaders) {
      this.okapiHeaders = ImmutableMap.copyOf(okapiHeaders);
      return this;
    }

    public Request build() {
      return new Request(metadataPrefix, identifier, from, until, set, resumptionToken, okapiHeaders);
    }
  }


  private Request(String metadataPrefix, String identifier, String from, String until, String set, String resumptionToken, Map<String, String> okapiHeaders) {
    this.metadataPrefix = metadataPrefix;
    this.identifier = identifier;
    this.from = from;
    this.until = until;
    this.set = set;
    this.resumptionToken = resumptionToken;
    this.okapiHeaders = okapiHeaders;
  }

  public String getMetadataPrefix() {
    return metadataPrefix;
  }

  public String getIdentifier() {
    return identifier;
  }

  public String getFrom() {
    return from;
  }

  public String getUntil() {
    return until;
  }

  public String getSet() {
    return set;
  }

  public String getResumptionToken() {
    return resumptionToken;
  }

  public Map<String, String> getOkapiHeaders() {
    return okapiHeaders;
  }

  /**
   * Factory method returning an instance of the builder.
   * @return {@link Builder} instance
   */
  public static Builder builder() {
    return new Builder();
  }
}
