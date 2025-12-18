CREATE UNIQUE INDEX IF NOT EXISTS error_logs_unique_idx ON ${myuniversity}_${mymodule}.errors (
  request_id,
  instance_id,
  md5(error_msg)
);
