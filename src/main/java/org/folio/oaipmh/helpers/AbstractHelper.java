package org.folio.oaipmh.helpers;

import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.ObjectFactory;
import org.openarchives.oai._2.VerbType;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Abstract helper implementation that provides some common methods.
 */
public abstract class AbstractHelper implements VerbHelper {
  public static final String REPOSITORY_BASE_URL = "repository.baseURL";

  protected ObjectFactory objectFactory = new ObjectFactory();

  /**
   * Creates basic {@link OAIPMH} with ResponseDate and Request details
   * @param verb {@link VerbType}
   * @return basic {@link OAIPMH}
   */
  protected OAIPMH buildBaseResponse(VerbType verb) {
    return objectFactory.createOAIPMH()
      // According to spec the nanoseconds should not be used so truncate to seconds
      .withResponseDate(Instant.now().truncatedTo(ChronoUnit.SECONDS))
      .withRequest(objectFactory.createRequestType()
        .withVerb(verb)
        .withValue(getBaseURL()));
  }

  /**
   * Return the repository base URL.
   * For now, it is based on System property, but later it might be pulled from mod-configuration.
   *
   * @return repository base URL
   */
  protected String getBaseURL() {
    String baseUrl = System.getProperty(REPOSITORY_BASE_URL);
    if (baseUrl == null) {
      throw new IllegalStateException("The required repository config 'repository.baseURL' is missing");
    }
    return baseUrl;
  }
}
