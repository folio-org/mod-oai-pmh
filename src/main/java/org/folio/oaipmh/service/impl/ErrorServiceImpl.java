package org.folio.oaipmh.service.impl;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.oaipmh.dao.ErrorsDao;
import org.folio.oaipmh.service.ErrorService;
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
public class ErrorServiceImpl implements ErrorService {

  private static final Logger logger = LogManager.getLogger(ErrorServiceImpl.class);
  private static final String LOCAL_ERROR_STORAGE_DIR = "local_error_storage";

  private FolioS3Client folioS3Client;
  private ErrorsDao errorsDao;
  private InstancesService instancesService;
  private static final Map<String, List<Future>> allSavedErrorsByRequestId = new ConcurrentHashMap<>();

  @Override
  public void logLocally(String tenantId, String requestId, String instanceId, String errorMsg) {
    if (isNull(requestId)) {
      logger.error("Request id cannot be null while saving the error: {}. Error was not saved.", errorMsg);
      return;
    }
    Errors errors = new Errors().setRequestId(UUID.fromString(requestId)).setInstanceFdd(instanceId)
      .setErrorMsg(errorMsg);
    Future savedError = errorsDao.saveErrors(errors, tenantId)
      .onComplete(h -> logger.debug("Error {} saved into DB. Instance id {}, request id {}.", errorMsg, instanceId, requestId));
    allSavedErrorsByRequestId.computeIfAbsent(requestId, list -> new ArrayList<>()).add(savedError);
  }

  @Override
  public Future<RequestMetadataLb> saveErrorsAndUpdateRequestMetadata(String tenantId, String requestId, RequestMetadataLb requestMetadata) {
    return CompositeFuture.all(allSavedErrorsByRequestId.get(requestId)).compose(h ->
      getErrorsAndSaveToLocalFile(requestId, tenantId)
        .compose(handler -> saveErrorFileToS3(requestId, tenantId, requestMetadata)))
      .compose(handler -> {
        allSavedErrorsByRequestId.remove(requestId);
        logger.info("error saved into S3: {}", handler.getRequestId());
        return Future.succeededFuture();
      });
  }

  private Future<Void> getErrorsAndSaveToLocalFile(String requestId, String tenantId) {
    return errorsDao.getErrorsList(requestId, tenantId).onComplete(listErrorsHandler -> {
      if (listErrorsHandler.succeeded()) {
        var listErrors = listErrorsHandler.result();
        logger.info("List of errors received from DB: " + listErrors.size());
        listErrors.forEach(error -> {
          var errorRow = error.getRequestId() + "," + error.getInstanceFdd() + "," + error.getErrorMsg();
          try {
            var localErrorDirPath = Path.of(LOCAL_ERROR_STORAGE_DIR);
            Files.createDirectories(localErrorDirPath);
            Files.write(Path.of(LOCAL_ERROR_STORAGE_DIR + File.separator + requestId + "-error.csv"),
              errorRow.getBytes(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
          } catch (IOException e) {
            e.printStackTrace();
            logger.error(LOCAL_ERROR_FILE_SAVE_FAILED, e.getMessage());
          }
        });
      } else {
        listErrorsHandler.cause().printStackTrace();
        logger.error(LOCAL_ERROR_FILE_GET_FAILED, listErrorsHandler.cause().getMessage());
      }
    }).compose(errors -> Future.succeededFuture());
  }

  private Future<RequestMetadataLb> saveErrorFileToS3(String requestId, String tenantId, RequestMetadataLb requestMetadata) {
    var localErrorDirPath = Path.of(LOCAL_ERROR_STORAGE_DIR);
    try {
      try {
        var errorPaths = Files.list(localErrorDirPath);
        var errorPathOpt = errorPaths.filter(path -> path.toString().contains(requestId)).findFirst();
        if (errorPathOpt.isPresent()) {
          var errorPath = errorPathOpt.get();
          var fileName = folioS3Client.write(errorPath.getFileName().toString(), Files.newInputStream(errorPath));
          var link = folioS3Client.getPresignedUrl(fileName);
          if (isNull(requestMetadata)) {
            return instancesService.updateRequestMetadataByLinkToError(requestId, tenantId, link).compose(res -> Future.succeededFuture());
          } else {
            requestMetadata.setLinkToErrorFile(link);
          }
        } else {
          logger.info(LOCAL_ERROR_FILE_NOT_FOUND);
        }
      } catch (IOException e) {
        logger.info(S3_ERROR_FILE_SAVE_FAILED, e.getMessage());
        e.printStackTrace();
      }
      return instancesService.saveRequestMetadata(requestMetadata, tenantId);
    } finally {
      deleteLocalErrorStorage(localErrorDirPath);
    }
  }

  private void deleteLocalErrorStorage(Path localErrorDirPath) {
    try {
      Files.list(localErrorDirPath).forEach(file -> {
        try {
          Files.deleteIfExists(file);
        } catch (IOException e) {
          logger.error(LOCAL_ERROR_FILE_DELETE_FAILED, file, e.getMessage());
        }
      });
      Files.deleteIfExists(localErrorDirPath);
    } catch (IOException e) {
      logger.error(LOCAL_ERROR_DIRECTORY_DELETE_FAILED, localErrorDirPath, e.getCause().getMessage());
    }
  }

  @Autowired
  public void setFolioS3Client(FolioS3Client folioS3Client) {
    this.folioS3Client = folioS3Client;
  }

  @Autowired
  public void setErrorsDao(ErrorsDao errorsDao) {
    this.errorsDao = errorsDao;
  }

  @Autowired
  public void setInstancesService(InstancesService instancesService) {
    this.instancesService = instancesService;
  }
}
