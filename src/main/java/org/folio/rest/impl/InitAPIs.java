package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.oaipmh.ResponseHelper;
import org.folio.rest.resource.interfaces.InitAPI;
import org.openarchives.oai._2_0.oai_identifier.OaiIdentifier;

import java.io.File;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

import static org.folio.oaipmh.helpers.VerbHelper.IDENTIFIER_PREFIX;
import static org.folio.oaipmh.helpers.VerbHelper.REPOSITORY_BASE_URL;

/**
 * The class initializes system properties and checks if required configs are specified
 */
public class InitAPIs implements InitAPI {
  private final Logger logger = LoggerFactory.getLogger(InitAPIs.class);

  private static final String CONFIG_PATH = "config" + File.separator + "config.properties";

  @Override
  public void init(Vertx vertx, Context context, Handler<AsyncResult<Boolean>> resultHandler) {
    String configPath = System.getProperty("configPath", CONFIG_PATH);
    try (InputStream configFile = InitAPIs.class.getClassLoader().getResourceAsStream(configPath)) {
      if (configFile == null) {
        throw new IllegalStateException(String.format("The config file %s is missing.", configPath));
      }
      final Properties confProperties = new Properties();
      confProperties.load(configFile);

      // Set system property from config file if no specified at runtime
      Properties sysProps = System.getProperties();
      confProperties.forEach((key, value) -> {
        if (sysProps.putIfAbsent(key, value) == null) {
          logger.info(String.format("The '%s' property was loaded from config file with its default value '%s'", key, value));
        }
      });

      // Initialize ResponseWriter and check if jaxb marshaller is ready to operate
      if (!ResponseHelper.getInstance().isJaxbInitialized()) {
        throw new IllegalStateException("The jaxb marshaller failed initialization.");
      }

      // Initialize data for OAI-PMH endpoint helpers
      context.config().put(IDENTIFIER_PREFIX, createRepositoryIdentifierPrefix());
      OaiPmhImpl.init(resultHandler::handle);
    } catch (Exception e) {
      resultHandler.handle(Future.failedFuture(e));
      logger.error("Unable to populate system properties", e);
    }
  }


  /**
   * Creates oai-identifier prefix based on {@link OaiIdentifier}. The format is {@literal oai:<repositoryIdentifier>}.
   * The repositoryIdentifier is based on repository base URL (host part).
   * @see <a href="http://www.openarchives.org/OAI/2.0/guidelines-oai-identifier.htm">OAI Identifier Format</a>
   */
  private String createRepositoryIdentifierPrefix() {
    String baseURL = System.getProperty(REPOSITORY_BASE_URL);
    try {
      URL url = new URL(baseURL);
      OaiIdentifier oaiIdentifier = new OaiIdentifier();
      oaiIdentifier.setRepositoryIdentifier(url.getHost());

      return oaiIdentifier.getScheme() + oaiIdentifier.getDelimiter() + oaiIdentifier.getRepositoryIdentifier() + oaiIdentifier.getDelimiter();
    } catch (MalformedURLException e) {
      throw new IllegalStateException(e);
    }
  }
}
