//package org.folio.oaipmh.helpers;
//
//import static org.folio.oaipmh.Constants.DEFLATE;
//import static org.folio.oaipmh.Constants.GZIP;
//import static org.folio.oaipmh.Constants.OKAPI_TENANT;
//import static org.folio.oaipmh.Constants.REPOSITORY_ADMIN_EMAILS;
//import static org.folio.oaipmh.Constants.REPOSITORY_DELETED_RECORDS;
//import static org.folio.oaipmh.Constants.REPOSITORY_NAME;
//import static org.folio.oaipmh.Constants.REPOSITORY_PROTOCOL_VERSION_2_0;
//import static org.folio.oaipmh.Constants.REPOSITORY_TIME_GRANULARITY;
//
//import java.net.MalformedURLException;
//import java.net.URL;
//import java.time.Instant;
//import java.time.temporal.ChronoUnit;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.concurrent.CompletableFuture;
//
//import javax.ws.rs.core.Response;
//
//import org.apache.commons.lang3.StringUtils;
//import org.folio.oaipmh.Request;
//import org.openarchives.oai._2.DeletedRecordType;
//import org.openarchives.oai._2.DescriptionType;
//import org.openarchives.oai._2.GranularityType;
//import org.openarchives.oai._2.IdentifyType;
//import org.openarchives.oai._2.OAIPMH;
//import org.openarchives.oai._2_0.oai_identifier.OaiIdentifier;
//
//import io.vertx.core.Context;
//import io.vertx.core.logging.Logger;
//import io.vertx.core.logging.LoggerFactory;
//import me.escoffier.vertx.completablefuture.VertxCompletableFuture;
//
//
///**
// * Helper class that contains business logic for retrieving OAI-PMH repository info.
// */
//public class GetOaiRepositoryInfoHelper extends AbstractHelper {
//
//  private static final Logger logger = LoggerFactory.getLogger(GetOaiRepositoryInfoHelper.class);
//
//  private static final String STORAGE_IDENTIFIER_SAMPLE = "3c4ae3f3-b460-4a89-a2f9-78ce3145e4fc";
//
//  @Override
//  public CompletableFuture<Response> handle(Request request, Context ctx) {
//    CompletableFuture<Response> future = new VertxCompletableFuture<>(ctx);
//    try {
//      String tenant = request.getOkapiHeaders().get(OKAPI_TENANT);
//      OAIPMH oai = getResponseHelper().buildBaseOaipmhResponse(request)
//        .withIdentify(new IdentifyType()
//          .withRepositoryName(getRepositoryName(tenant))
//          .withBaseURL(request.getOaiRequest().getValue())
//          .withProtocolVersion(REPOSITORY_PROTOCOL_VERSION_2_0)
//          .withEarliestDatestamp(getEarliestDatestamp())
//          .withGranularity(GranularityType.fromValue(RepositoryConfigurationUtil.getProperty
//            (tenant, REPOSITORY_TIME_GRANULARITY)))
//          .withDeletedRecord(DeletedRecordType.fromValue(RepositoryConfigurationUtil
//            .getProperty(tenant, REPOSITORY_DELETED_RECORDS)))
//          .withAdminEmails(getEmails(tenant))
//          .withCompressions(GZIP, DEFLATE)
//          .withDescriptions(getDescriptions(request)));
//
//      future.complete(getResponseHelper().buildSuccessResponse(oai));
//    } catch (Exception e) {
//      logger.error("Error happened while processing Identify verb request", e);
//      future.completeExceptionally(e);
//    }
//    return future;
//  }
//
//  /**
//   * Return the earliest repository datestamp.
//   * For now, it always returns epoch instant, but later it might be pulled from mod-configuration.
//   *
//   * @return repository earliest datestamp
//   */
//  private Instant getEarliestDatestamp() {
//    return Instant.EPOCH.truncatedTo(ChronoUnit.SECONDS);
//  }
//
//  /**
//   * Return the repository name.
//   *
//   * @return repository name
//   */
//  private String getRepositoryName(String tenant) {
//    String repoName = RepositoryConfigurationUtil.getProperty(tenant, REPOSITORY_NAME);
//    if (repoName == null) {
//      throw new IllegalStateException("The required repository config 'repository.name' is missing");
//    }
//    return repoName;
//  }
//
//  /**
//   * Return the array of repository admin e-mails.
//   * For now, it is based on System property, but later it might be pulled from mod-configuration.
//   *
//   * @return repository name
//   */
//  private String[] getEmails(String tenant) {
//    String emails = RepositoryConfigurationUtil.getProperty(tenant, REPOSITORY_ADMIN_EMAILS);
//    if (StringUtils.isBlank(emails)) {
//      throw new IllegalStateException("The required repository config 'repository.adminEmails' is missing");
//    }
//    return emails.split(",");
//  }
//
//  /**
//   * Returns list of the {@link DescriptionType} elements.
//   * For now only oai-identifier description is created.
//   *
//   * @param request the OAI-PMH request holder
//   * @return list of the {@link DescriptionType} elements
//   */
//  private List<DescriptionType> getDescriptions(Request request) throws MalformedURLException {
//    List<DescriptionType> descriptions = new ArrayList<>();
//    descriptions.add(buildOaiIdentifierDescription(request));
//    return descriptions;
//  }
//
//  /**
//   * Creates oai-identifier description
//
//   * @param request the OAI-PMH request holder
//   * @return oai-identifier {@link DescriptionType} elements
//   */
//  private DescriptionType buildOaiIdentifierDescription(Request request) throws MalformedURLException {
//    OaiIdentifier oaiIdentifier = new OaiIdentifier();
//    oaiIdentifier.setRepositoryIdentifier(new URL(request.getOaiRequest().getValue()).getHost());
//    oaiIdentifier.setSampleIdentifier(getIdentifier(request.getIdentifierPrefix(), STORAGE_IDENTIFIER_SAMPLE));
//    return new DescriptionType().withAny(oaiIdentifier);
//  }
//}
