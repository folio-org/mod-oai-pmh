buildMvn {
  publishModDescriptor = 'no'
  publishAPI = 'yes'
  mvnDeploy = 'no'
  runLintRamlCop = 'yes'

  doDocker = {
    buildJavaDocker {
      publishMaster = 'no'
      healthChk = 'no'
      healthChkCmd = 'curl -sS --fail -o /dev/null  http://localhost:8081/apidocs/ || exit 1'
    }
  }
}
