package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.oaipmh.ResponseHelper;
import org.folio.rest.jaxrs.resource.Oai;
import org.openarchives.oai._2.DeletedRecordType;
import org.openarchives.oai._2.GranularityType;
import org.openarchives.oai._2.OAIPMH;
import org.openarchives.oai._2.OAIPMHerrorcodeType;
import org.openarchives.oai._2.ObjectFactory;
import org.openarchives.oai._2.VerbType;

import javax.ws.rs.core.Response;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;

import static io.vertx.core.Future.succeededFuture;
import static org.folio.rest.RestVerticle.OKAPI_HEADER_TENANT;

public class OaiPmhImpl implements Oai {
  static final String REPOSITORY_NAME = "repository.name";
  static final String REPOSITORY_BASE_URL = "repository.baseURL";
  static final String REPOSITORY_ADMIN_EMAILS = "repository.adminEmails";
  private static final String REPOSITORY_PROTOCOL_VERSION = "repository.protocolVersion";
  static final String REPOSITORY_PROTOCOL_VERSION_2_0 = "2.0";
  private final Logger logger = LoggerFactory.getLogger("mod-oai-pmh");
  private ObjectFactory objectFactory = new ObjectFactory();

  @Override
  public void getOaiRecords(String resumptionToken, String from, String until, String set, String metadataPrefix,
                            Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
                            Context vertxContext) {
    OAIPMH oai = buildBaseResponse(VerbType.LIST_RECORDS)
      .withErrors(objectFactory.createOAIPMHerrorType().withCode(OAIPMHerrorcodeType.NO_RECORDS_MATCH));
    oai.getRequest()
       .withResumptionToken(resumptionToken)
       //.withFrom(from != null ? Instant.parse(from) : null)
       //.withUntil(until != null ? Instant.parse(until) : null)
       .withSet(set)
       .withMetadataPrefix(metadataPrefix);

    try {
      String response = ResponseHelper.getInstance().writeToString(oai);
      asyncResultHandler.handle(succeededFuture(GetOaiRecordsResponse.respond422WithApplicationXml(response)));
    } catch (Exception e) {
      logger.error("Unexpected error happened while processing ListRecords verb request", e);
      asyncResultHandler.handle(succeededFuture(GetOaiRecordsResponse.respond500WithTextPlain(getErrorMessage())));
    }
  }

  @Override
  public void getOaiRecordsById(String id, String metadataPrefix, Map<String, String> okapiHeaders,
                                Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    OAIPMH oai = buildBaseResponse(VerbType.GET_RECORD)
      .withErrors(objectFactory.createOAIPMHerrorType().withCode(OAIPMHerrorcodeType.ID_DOES_NOT_EXIST));
    oai.getRequest().withIdentifier(id).withMetadataPrefix(metadataPrefix);

    try {
      String response = ResponseHelper.getInstance().writeToString(oai);
      asyncResultHandler.handle(succeededFuture(GetOaiRecordsByIdResponse.respond422WithApplicationXml(response)));
    } catch (Exception e) {
      logger.error("Unexpected error happened while processing GetRecord verb request", e);
      asyncResultHandler.handle(succeededFuture(GetOaiRecordsByIdResponse.respond500WithTextPlain(getErrorMessage())));
    }
  }

  @Override
  public void getOaiIdentifiers(String resumptionToken, String from, String until, String set, String metadataPrefix,
                                Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
                                Context vertxContext) {
    asyncResultHandler.handle(succeededFuture(GetOaiIdentifiersResponse.respond500WithTextPlain("The verb ListIdentifiers is not supported yet :(")));
  }

  @Override
  public void getOaiMetadataFormats(String identifier, Map<String, String> okapiHeaders,
                                    Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    asyncResultHandler.handle(succeededFuture(GetOaiMetadataFormatsResponse.respond500WithTextPlain("The verb ListMetadataFormats is not supported yet :(")));
  }

  @Override
  public void getOaiSets(String resumptionToken, Map<String, String> okapiHeaders,
                         Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    asyncResultHandler.handle(succeededFuture(GetOaiSetsResponse.respond500WithTextPlain("The verb ListSets is not supported yet :(")));
  }

  @Override
  public void getOaiRepositoryInfo(Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
                                   Context vertxContext) {
    try {
      String repoName = System.getProperty(REPOSITORY_NAME);
      String baseUrl = System.getProperty(REPOSITORY_BASE_URL);
      String emails = System.getProperty(REPOSITORY_ADMIN_EMAILS);
      if (repoName == null || baseUrl == null || emails == null) {
        throw new IllegalStateException(String.format("One or more of the required repository configs missing: " +
          "{repository.name: %s, repository.baseURL: %s, repository.adminEmails: %s}", repoName, baseUrl, emails));
      }

      OAIPMH oai = buildBaseResponse(VerbType.IDENTIFY)
        .withIdentify(objectFactory.createIdentifyType()
                                   .withRepositoryName(repoName + "_" + okapiHeaders.get(OKAPI_HEADER_TENANT))
                                   .withBaseURL(baseUrl)
                                   .withProtocolVersion(System.getProperty(REPOSITORY_PROTOCOL_VERSION, REPOSITORY_PROTOCOL_VERSION_2_0))
                                   .withEarliestDatestamp(Instant.EPOCH.truncatedTo(ChronoUnit.SECONDS))
                                   .withGranularity(GranularityType.YYYY_MM_DD_THH_MM_SS_Z)
                                   .withDeletedRecord(DeletedRecordType.NO)
                                   .withAdminEmails(emails.split(",")));
      String response = ResponseHelper.getInstance().writeToString(oai);
      asyncResultHandler.handle(succeededFuture(GetOaiRepositoryInfoResponse.respond200WithApplicationXml(response)));
    } catch (Exception e) {
      logger.error("Error happened while processing Identify verb request", e);
      asyncResultHandler.handle(succeededFuture(GetOaiRepositoryInfoResponse.respond500WithTextPlain(getErrorMessage())));
    }
  }

  /**
   * Generates user friendly error message in case if the error happens while the requiest is being processed
   * @return user friendly error message in case if the error happens while the requiest is being processed
   */
  private String getErrorMessage() {
    String errorMsg = "Sorry, we can't process your request.";
    String emails = System.getProperty("repository.adminEmails");
    if (emails != null && !emails.isEmpty()) {
      errorMsg += " Please contact administrator(s): " + emails;
    }
    return errorMsg;
  }

  /**
   * Creates basic {@link OAIPMH} with ResponseDate and Request details
   * @param verb {@link VerbType}
   * @return basic {@link OAIPMH}
   */
  private OAIPMH buildBaseResponse(VerbType verb) {
    return objectFactory.createOAIPMH()
                        // According to spec the nanoseconds should not be used so truncate to seconds
                        .withResponseDate(Instant.now().truncatedTo(ChronoUnit.SECONDS))
                        .withRequest(objectFactory.createRequestType()
                                                  .withVerb(verb)
                                                  .withValue(System.getProperty("repository.baseURL")));
  }
}
