package org.folio.oaipmh.helpers;

import io.vertx.core.Context;
import me.escoffier.vertx.completablefuture.VertxCompletableFuture;
import org.apache.log4j.Logger;
import org.folio.oaipmh.Request;
import org.folio.oaipmh.ResponseHelper;
import org.folio.rest.jaxrs.resource.Oai.GetOaiRepositoryInfoResponse;
import org.openarchives.oai._2.DeletedRecordType;
import org.openarchives.oai._2.GranularityType;
import org.openarchives.oai._2.IdentifyType;
import org.openarchives.oai._2.OAIPMH;

import javax.ws.rs.core.Response;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.folio.oaipmh.Constants.GZIP;
import static org.folio.oaipmh.Constants.DEFLATE;
import static org.folio.okapi.common.XOkapiHeaders.TENANT;


/**
 * Helper class that contains business logic for retrieving OAI-PMH repository info.
 */
public class GetOaiRepositoryInfoHelper extends AbstractHelper {

  private static final Logger logger = Logger.getLogger(GetOaiRepositoryInfoHelper.class);

  public static final String REPOSITORY_NAME = "repository.name";
  public static final String REPOSITORY_ADMIN_EMAILS = "repository.adminEmails";
  public static final String REPOSITORY_PROTOCOL_VERSION_2_0 = "2.0";


  @Override
  public CompletableFuture<Response> handle(Request request, Context ctx) {
    CompletableFuture<Response> future = new VertxCompletableFuture<>(ctx);
    try {
      OAIPMH oai = buildBaseResponse(request)
        .withIdentify(new IdentifyType()
          .withRepositoryName(getRepositoryName(request.getOkapiHeaders()))
          .withBaseURL(getBaseURL())
          .withProtocolVersion(REPOSITORY_PROTOCOL_VERSION_2_0)
          .withEarliestDatestamp(getEarliestDatestamp())
          .withGranularity(GranularityType.YYYY_MM_DD_THH_MM_SS_Z)
          .withDeletedRecord(DeletedRecordType.NO)
          .withAdminEmails(getEmails())
          .withCompressions(GZIP, DEFLATE));

      String response = ResponseHelper.getInstance().writeToString(oai);
      future.complete(GetOaiRepositoryInfoResponse.respond200WithApplicationXml(response));
    } catch (Exception e) {
      logger.error("Error happened while processing Identify verb request", e);
      future.completeExceptionally(e);
    }

    return future;
  }

  /**
   * Return the repository name.
   * For now, it is based on System property, but later it might be pulled from mod-configuration.
   *
   * @return repository name
   */
  private String getRepositoryName(Map<String, String> okapiHeaders) {
    String repoName = System.getProperty(REPOSITORY_NAME);
    if (repoName == null) {
      throw new IllegalStateException("The required repository config 'repository.name' is missing");
    }
    return repoName + "_" + okapiHeaders.get(TENANT);
  }

  /**
   * Return the earliest repository datestamp.
   * For now, it always returns epoch instant, but later it might be pulled from mod-configuration.
   *
   * @return earliest datestamp
   */
  private Instant getEarliestDatestamp() {
    return Instant.EPOCH.truncatedTo(ChronoUnit.SECONDS);
  }

  /**
   * Return the array of repository admin e-mails.
   * For now, it is based on System property, but later it might be pulled from mod-configuration.
   *
   * @return repository name
   */
  private String[] getEmails() {
    String emails = System.getProperty(REPOSITORY_ADMIN_EMAILS);
    if (emails == null) {
      throw new IllegalStateException("The required repository config 'repository.adminEmails' is missing");
    }
    return emails.split(",");
  }
}
