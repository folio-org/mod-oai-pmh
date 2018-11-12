package org.folio.oaipmh.helpers;

import io.vertx.core.Context;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import me.escoffier.vertx.completablefuture.VertxCompletableFuture;
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
import java.util.concurrent.CompletableFuture;

import static org.folio.oaipmh.Constants.REPOSITORY_ADMIN_EMAILS;
import static org.folio.oaipmh.Constants.REPOSITORY_BASE_URL;
import static org.folio.oaipmh.Constants.REPOSITORY_DELETED_RECORDS;
import static org.folio.oaipmh.Constants.REPOSITORY_NAME;
import static org.folio.oaipmh.Constants.REPOSITORY_PROTOCOL_VERSION;
import static org.folio.oaipmh.Constants.REPOSITORY_TIME_GRANULARITY;
import static org.folio.oaipmh.Constants.GZIP;
import static org.folio.oaipmh.Constants.DEFLATE;
import static org.folio.oaipmh.Constants.OKAPI_TENANT;


/**
 * Helper class that contains business logic for retrieving OAI-PMH repository info.
 */
public class GetOaiRepositoryInfoHelper extends AbstractHelper {

  private static final Logger logger = LoggerFactory.getLogger(GetOaiRepositoryInfoHelper.class);

  public static final String REPOSITORY_PROTOCOL_VERSION_2_0 = "2.0";


  @Override
  public CompletableFuture<Response> handle(Request request, Context ctx) {
    CompletableFuture<Response> future = new VertxCompletableFuture<>(ctx);
    try {
      OAIPMH oai = buildBaseResponse(request)
        .withIdentify(new IdentifyType()
          .withRepositoryName(RepositoryConfigurationHelper.getProperty(REPOSITORY_NAME, ctx))
          .withBaseURL(RepositoryConfigurationHelper.getProperty(REPOSITORY_BASE_URL, ctx))
          .withProtocolVersion(REPOSITORY_PROTOCOL_VERSION_2_0)
          .withEarliestDatestamp(getEarliestDatestamp())
          .withGranularity(GranularityType.fromValue(RepositoryConfigurationHelper.getProperty
            (REPOSITORY_TIME_GRANULARITY, ctx)))
          .withDeletedRecord(DeletedRecordType.fromValue(RepositoryConfigurationHelper
            .getProperty(REPOSITORY_DELETED_RECORDS, ctx)))
          .withAdminEmails(getEmails(ctx))
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
  private String[] getEmails(Context ctx) {
    String emails = RepositoryConfigurationHelper.getProperty(REPOSITORY_ADMIN_EMAILS, ctx);
    if (emails == null) {
      throw new IllegalStateException("The required repository config 'repository.adminEmails' is missing");
    }
    return emails.split(",");
  }
}
