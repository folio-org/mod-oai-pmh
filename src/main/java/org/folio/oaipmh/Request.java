package org.folio.oaipmh;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toMap;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;
import static org.folio.oaipmh.Constants.OKAPI_TOKEN;
import static org.folio.oaipmh.Constants.OKAPI_URL;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.folio.rest.tools.utils.TenantTool;
import org.openarchives.oai._2.RequestType;
import org.openarchives.oai._2.VerbType;
import org.openarchives.oai._2_0.oai_identifier.OaiIdentifier;

/**
 * Class that represents OAI-PMH request and holds http query arguments.
 * It implements builder pattern, so use {@link Builder} instance to build an instance of the request.
 */
public class Request {
  private static final char PARAMETER_SEPARATOR = '&';
  private static final String PARAMETER_VALUE_SEPARATOR = "=";

  private final RequestType oaiRequest;
  private final Map<String, String> okapiHeaders;
  private final String tenant;
  private final String okapiToken;
  private final String okapiUrl;

  /** The request restored from resumptionToken. */
  private RequestType restoredOaiRequest;
  /** The result offset used for partitioning. */
  private int offset;
  /** The previous total number of records used for partitioning. */
  private int totalRecords;
  /** The id of the first record in the next set of results used for partitioning. */
  private String nextRecordId;

  /**
   * Builder used to build the request.
   */
  public static class Builder {
    private final RequestType oaiRequest = new RequestType();
    private Map<String, String> okapiHeaders;

    public Builder verb(VerbType verb) {
      oaiRequest.setVerb(verb);
      return this;
    }

    public Builder metadataPrefix(String metadataPrefix) {
      oaiRequest.setMetadataPrefix(metadataPrefix);
      return this;
    }

    public Builder identifier(String identifier) {
      oaiRequest.setIdentifier(identifier);
      return this;
    }

    public Builder from(String from) {
      oaiRequest.setFrom(from);
      return this;
    }

    public Builder until(String until) {
      oaiRequest.setUntil(until);
      return this;
    }

    public Builder set(String set) {
      oaiRequest.setSet(set);
      return this;
    }

    public Builder resumptionToken(String resumptionToken) {
      oaiRequest.setResumptionToken(resumptionToken);
      return this;
    }

    public Builder okapiHeaders(Map<String, String> okapiHeaders) {
      this.okapiHeaders = okapiHeaders;
      return this;
    }

    public Request build() {
      return new Request(oaiRequest, okapiHeaders);
    }


    public Builder baseURL(String baseURL) {
      oaiRequest.setValue(baseURL);
      return this;
    }
  }


  private Request(RequestType oaiRequest, Map<String, String> okapiHeaders) {
    this.oaiRequest = oaiRequest;
    this.okapiHeaders = okapiHeaders;
    this.tenant = okapiHeaders.get(OKAPI_TENANT);
    this.okapiToken = okapiHeaders.get(OKAPI_TOKEN);
    this.okapiUrl = okapiHeaders.get(OKAPI_URL);
  }


  public String getMetadataPrefix() {
    return restoredOaiRequest != null ? restoredOaiRequest.getMetadataPrefix() : oaiRequest.getMetadataPrefix();
  }

  public String getIdentifier() {
    return restoredOaiRequest != null ? restoredOaiRequest.getIdentifier() : oaiRequest.getIdentifier();
  }


  public String getFrom() {
    return restoredOaiRequest != null ? restoredOaiRequest.getFrom() : oaiRequest.getFrom();
  }

  public String getUntil() {
    return restoredOaiRequest != null ? restoredOaiRequest.getUntil() : oaiRequest.getUntil();
  }

  public String getSet() {
    return restoredOaiRequest != null ? restoredOaiRequest.getSet() : oaiRequest.getSet();
  }

  public String getResumptionToken() {
    return oaiRequest.getResumptionToken();
  }

  public VerbType getVerb() {
    return oaiRequest.getVerb();
  }

  public RequestType getOaiRequest() {
    return oaiRequest;
  }

  public String getStorageIdentifier() {
    return getIdentifier().substring(getIdentifierPrefix().length()) ;
  }

  public String getIdentifierPrefix() {
    try {
      String tenantId = TenantTool.tenantId(okapiHeaders);
      URL url = new URL(oaiRequest.getValue());
      OaiIdentifier oaiIdentifier = new OaiIdentifier();
      oaiIdentifier.setRepositoryIdentifier(url.getHost());
      return oaiIdentifier.getScheme() + oaiIdentifier.getDelimiter()
        + oaiIdentifier.getRepositoryIdentifier()
        + oaiIdentifier.getDelimiter() + tenantId + "/";
    } catch (MalformedURLException e) {
      throw new IllegalStateException(e);
    }
  }

  public Map<String, String> getOkapiHeaders() {
    return okapiHeaders;
  }

  public int getOffset() {
    return offset;
  }

  public int getTotalRecords() {
    return totalRecords;
  }

  public String getNextRecordId() {
    return nextRecordId;
  }

  public String getTenant() {
    return tenant;
  }

  public String getOkapiToken() {
    return okapiToken;
  }

  public String getOkapiUrl() {
    return okapiUrl;
  }

  /**
   * Factory method returning an instance of the builder.
   * @return {@link Builder} instance
   */
  public static Builder builder() {
    return new Builder();
  }




  /**
   * Restores original request encoded in resumptionToken.
   * The resumptionToken is exclusive param, so the request cannot be restored if some other params are provided
   * in the request along with the resumptionToken.
   *
   * @return true if the request was restored, false otherwise.
   */
  public boolean restoreFromResumptionToken() {
    if (oaiRequest.getResumptionToken() == null || !isResumptionTokenExclusive()) {
      return false;
    }

    String resumptionToken = new String(Base64.getUrlDecoder().decode(oaiRequest.getResumptionToken()),
      StandardCharsets.UTF_8);

    Map<String, String> params = URLEncodedUtils
      .parse(resumptionToken, UTF_8, PARAMETER_SEPARATOR).stream()
      .collect(toMap(NameValuePair::getName, NameValuePair::getValue));

    restoredOaiRequest = new RequestType();
    restoredOaiRequest.setMetadataPrefix(params.get("metadataPrefix"));
    restoredOaiRequest.setFrom(params.get("from"));
    restoredOaiRequest.setUntil(params.get("until"));
    restoredOaiRequest.setSet(params.get("set"));
    this.offset = Integer.parseInt(params.get("offset"));
    this.totalRecords = Integer.parseInt(params.get("totalRecords"));
    this.nextRecordId = params.get("nextRecordId");

    return true;
  }

  /**
   * Indicates if this request is restored from resumptionToken.
   * @return true if restored from resumption token, false otherwise
   */
  public boolean isRestored() {
    return restoredOaiRequest != null;
  }

  /**
   * Serializes the request to resumptionToken string. Only original request params are serialized.
   * All extra parameters required to support partitioning should be additionally passed to the method.
   *
   * @param extraParams extra parameters used to support partitioning
   * @return serialized resumptionToken
   */
  public String toResumptionToken(Map<String, String> extraParams) {
    StringBuilder builder = new StringBuilder();
    appendParam(builder, "metadataPrefix", getMetadataPrefix());
    appendParam(builder, "from", getFrom());
    appendParam(builder, "until", getUntil());
    appendParam(builder, "set", getSet());

    extraParams.entrySet().stream()
      .map(e -> e.getKey() + PARAMETER_VALUE_SEPARATOR + e.getValue())
      .forEach(param -> builder.append(PARAMETER_SEPARATOR).append(param));

    return Base64.getUrlEncoder()
      .encodeToString(builder.toString().getBytes())
      // this is to remove '=' or '==' at the end
      .split("=")[0];
  }


  private void appendParam(StringBuilder builder, String name, String value) {
    if (value != null) {
      if (builder.length() > 0) {
        builder.append(PARAMETER_SEPARATOR);
      }
      builder.append(name).append(PARAMETER_VALUE_SEPARATOR).append(value);
    }
  }

  /**
   * Checks if the resumptionToken is provided exclusively by comparing this request
   * with one that only contains the resumptionToken.
   * @return true is resumptionToken is exclusive, false otherwise
   */
  private boolean isResumptionTokenExclusive() {
    Request exclusiveParamRequest = Request.builder()
      .resumptionToken(oaiRequest.getResumptionToken())
      .baseURL(oaiRequest.getValue())
      .verb(oaiRequest.getVerb())
      .build();

    return exclusiveParamRequest.getOaiRequest().equals(oaiRequest);
  }
}
