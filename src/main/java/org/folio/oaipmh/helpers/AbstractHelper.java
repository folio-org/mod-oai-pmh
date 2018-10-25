package org.folio.oaipmh.helpers;

import io.vertx.core.json.JsonObject;
import org.folio.oaipmh.Request;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.tools.client.HttpClientFactory;
import org.folio.rest.tools.client.interfaces.HttpClientInterface;
import org.folio.rest.tools.utils.TenantTool;
import org.openarchives.oai._2.HeaderType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.RequestType;
import org.openarchives.oai._2_0.oai_identifier.OaiIdentifier;

import java.net.MalformedURLException;
import java.net.URL;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;

/**
 * Abstract helper implementation that provides some common methods.
 */
public abstract class AbstractHelper implements VerbHelper {

  public static final String REPOSITORY_BASE_URL = "repository.baseURL";

  /**
   * The value should be populated only once at start up. So considered to be constant.
   * @see #createRepositoryIdentifierPrefix()
   */
  private static String OAI_IDENTIFIER_PREFIX;

  /**
   * Holds instance to handle items returned
   */
  protected static ItemsStorageHelper storageHelper;

  /**
   * Initializes data required for OAI-PMH responses
   */
  public static void init() {
    createRepositoryIdentifierPrefix();
    storageHelper = ItemsStorageHelper.getItemsStorageHelper();
  }

  /**
   * Creates basic {@link OAIPMH} with ResponseDate and Request details
   * @param request {@link Request}
   * @return basic {@link OAIPMH}
   */
  protected OAIPMH buildBaseResponse(RequestType request) {
    return new OAIPMH()
      // According to spec the nanoseconds should not be used so truncate to seconds
      .withResponseDate(Instant.now().truncatedTo(ChronoUnit.SECONDS))
      .withRequest(request.withValue(getBaseURL()));
  }

  /**
   * Return the repository base URL.
   * For now, it is based on System property, but later it might be pulled from mod-configuration.
   *
   * @return repository base URL
   */
  protected static String getBaseURL() {
    String baseUrl = System.getProperty(REPOSITORY_BASE_URL);
    if (baseUrl == null) {
      throw new IllegalStateException("The required repository config 'repository.baseURL' is missing");
    }
    return baseUrl;
  }

  /**
   * Creates oai-identifier prefix based on {@link OaiIdentifier}. The format is {@literal oai:<repositoryIdentifier>}.
   * The repositoryIdentifier is based on repository base URL (host part).
   * @see <a href="http://www.openarchives.org/OAI/2.0/guidelines-oai-identifier.htm">OAI Identifier Format</a>
   */
  private static void createRepositoryIdentifierPrefix() {
        String baseURL = getBaseURL();
    try {
      URL url = new URL(baseURL);
      OaiIdentifier oaiIdentifier = new OaiIdentifier();
      oaiIdentifier.setRepositoryIdentifier(url.getHost());

      OAI_IDENTIFIER_PREFIX = oaiIdentifier.getScheme() + oaiIdentifier.getDelimiter()
        + oaiIdentifier.getRepositoryIdentifier() + oaiIdentifier.getDelimiter();
    } catch (MalformedURLException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Return the repository name.
   * For now, it is based on System property, but later it might be pulled from mod-configuration.
   *
   * @return repository name
   */
  protected HttpClientInterface getOkapiClient(Map<String, String> okapiHeaders) {
    String tenantId = TenantTool.tenantId(okapiHeaders);
    String okapiURL = okapiHeaders.get(XOkapiHeaders.URL);
    return HttpClientFactory.getHttpClient(okapiURL, tenantId);
  }

  /**
   * Creates {@link HeaderType} and populates Identifier, Datestamp and Set
   *
   * @param instance the instance item returned by storage service
   * @return populated {@link HeaderType}
   */
  protected HeaderType populateHeader(JsonObject instance) {
    return new HeaderType()
      .withIdentifier(getIdentifier(instance))
      .withDatestamp(getInstanceDate(instance))
      .withSetSpecs("all");
  }

  /**
   * Returns last modified or created date
   * @param instance the instance item returned by storage service
   * @return {@link Instant} based on updated or created date
   */
  private Instant getInstanceDate(JsonObject instance) {
    return storageHelper.getLastModifiedDate(instance);
  }

  /**
   * Builds oai-identifier
   * @param instance the instance item returned by storage service
   * @return oai-identifier
   */
  private String getIdentifier(JsonObject instance) {
    return  OAI_IDENTIFIER_PREFIX + storageHelper.getItemId(instance);
  }
}
