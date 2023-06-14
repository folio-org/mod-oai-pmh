package org.folio.oaipmh.service.impl;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.dao.ErrorsDao;
import org.folio.oaipmh.service.ErrorsService;
import org.folio.oaipmh.service.InstancesService;
import org.folio.rest.jooq.tables.pojos.Errors;
import org.folio.rest.jooq.tables.pojos.RequestMetadataLb;
import org.folio.s3.client.FolioS3Client;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.isNull;
import static org.folio.oaipmh.Constants.LOCAL_ERROR_DIRECTORY_DELETE_FAILED;
import static org.folio.oaipmh.Constants.LOCAL_ERROR_FILE_DELETE_FAILED;
import static org.folio.oaipmh.Constants.LOCAL_ERROR_FILE_GET_FAILED;
import static org.folio.oaipmh.Constants.LOCAL_ERROR_FILE_NOT_FOUND;
import static org.folio.oaipmh.Constants.LOCAL_ERROR_FILE_SAVE_FAILED;
import static org.folio.oaipmh.Constants.S3_ERROR_FILE_SAVE_FAILED;

@Service
public class ErrorsServiceImpl implements ErrorsService {

  private static final Logger logger = LogManager.getLogger(ErrorsServiceImpl.class);
  private static final String LOCAL_ERROR_STORAGE_DIR = "local_error_storage";

  @Autowired
  private FolioS3Client folioS3Client;

  @Autowired
  private ErrorsDao errorsDao;

  @Autowired
  private InstancesService instancesService;

  @SuppressWarnings("rawtypes")
  private static final Map<String, List<Future>> allSavedErrorsByRequestId = new ConcurrentHashMap<>();

  @Override
  public void logLocally(String tenantId, String requestId, String instanceId, String errorMsg) {
    if (isNull(requestId) || isNull(tenantId) || isNull(instanceId) || isNull(errorMsg)) {
      logger.error("requestId ({}), or tenantId ({}), or instanceId ({}), or errorMsg ({}) cannot be null while saving the error: error was not saved.",
        requestId, tenantId, instanceId, errorMsg);
      return;
    }
    Errors errors = new Errors().setRequestId(UUID.fromString(requestId)).setInstanceId(instanceId)
      .setErrorMsg(errorMsg);
    var savedError = errorsDao.saveErrors(errors, tenantId)
      .onComplete(h -> logger.debug("Error {} saved into DB. Instance id {}, request id {}.", errorMsg, instanceId, requestId));
    allSavedErrorsByRequestId.computeIfAbsent(requestId, list -> new ArrayList<>()).add(savedError);
  }

  @Override
  public Future<RequestMetadataLb> saveErrorsAndUpdateRequestMetadata(String tenantId, String requestId, RequestMetadataLb requestMetadata) {
    return CompositeFuture.all(allSavedErrorsByRequestId.getOrDefault(requestId, List.of(Future.succeededFuture())))
      .compose(savedErrorsToDbCompleted -> getErrorsAndSaveToLocalFile(requestId, tenantId)
        .compose(savedErrorsToLocalFileCompleted -> saveErrorFileToS3(requestId, tenantId, requestMetadata)))
      .compose(savedErrorsToS3Completed -> {
        allSavedErrorsByRequestId.remove(requestId);
        logger.info("Processed RequestMetadataLb with request id = {}", savedErrorsToS3Completed.getRequestId());
        return Future.succeededFuture(savedErrorsToS3Completed);
      });
  }

  private Future<Void> getErrorsAndSaveToLocalFile(String requestId, String tenantId) {
    return errorsDao.getErrorsList(requestId, tenantId).onComplete(listErrorsHandler -> {
      if (listErrorsHandler.succeeded()) {
        var listErrors = listErrorsHandler.result();
        logger.debug("Number of errors received from DB: {}", listErrors.size());
        listErrors.forEach(error -> {
          var errorRow = error.getRequestId() + "," + error.getInstanceId() + "," + error.getErrorMsg() + System.lineSeparator();
          try {
            var localErrorDirPath = Path.of(LOCAL_ERROR_STORAGE_DIR);
            Files.createDirectories(localErrorDirPath);
            Files.write(Path.of(LOCAL_ERROR_STORAGE_DIR + File.separator + requestId + "-error.csv"),
              errorRow.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
          } catch (IOException e) {
            logger.error(LOCAL_ERROR_FILE_SAVE_FAILED, e.getMessage());
          }
        });
      } else {
        logger.error(LOCAL_ERROR_FILE_GET_FAILED, listErrorsHandler.cause().toString());
      }
    }).compose(errors -> Future.succeededFuture());
  }

  private Future<RequestMetadataLb> saveErrorFileToS3(String requestId, String tenantId, RequestMetadataLb requestMetadata) {
    var localErrorDirPath = Path.of(LOCAL_ERROR_STORAGE_DIR);
    try {
      if (Files.exists(localErrorDirPath)) {
        try (var errorPaths = Files.list(localErrorDirPath)) {
          var errorPathOpt = errorPaths.filter(path -> path.toString().contains(requestId)).findFirst();
          if (errorPathOpt.isPresent()) {
            var errorPath = errorPathOpt.get();
            var fileName = folioS3Client.write(errorPath.getFileName().toString(), Files.newInputStream(errorPath));
            var link = folioS3Client.getPresignedUrl(fileName);
            if (isNull(requestMetadata)) {
              return instancesService.updateRequestMetadataByLinkToError(requestId, tenantId, link);
            } else {
              requestMetadata.setLinkToErrorFile(link);
            }
          } else {
            logger.warn(LOCAL_ERROR_FILE_NOT_FOUND, requestId);
          }
        } catch (IOException e) {
          logger.error(S3_ERROR_FILE_SAVE_FAILED, e.toString());
        }
      }
      return isNull(requestMetadata) ?
        instancesService.updateRequestMetadataByLinkToError(requestId, tenantId, null) :
        instancesService.saveRequestMetadata(requestMetadata, tenantId);
    } finally {
      deleteLocalErrorStorage(localErrorDirPath);
    }
  }

  private void deleteLocalErrorStorage(Path localErrorDirPath) {
    if (Files.exists(localErrorDirPath)) {
      try (var listErrors = Files.list(localErrorDirPath)) {
        listErrors.forEach(file -> {
          try {
            Files.deleteIfExists(file);
          } catch (IOException e) {
            logger.error(LOCAL_ERROR_FILE_DELETE_FAILED, e.toString());
          }
        });
        Files.deleteIfExists(localErrorDirPath);
      } catch (IOException e) {
        logger.error(LOCAL_ERROR_DIRECTORY_DELETE_FAILED, e.getCause().toString());
      }
    }
  }
}
