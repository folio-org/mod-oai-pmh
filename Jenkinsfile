buildMvn {
  publishModDescriptor = 'no'
  publishAPI = 'no'
  mvnDeploy = 'no'
  runLintRamlCop = 'no'

  doDocker = {
    buildJavaDocker {
      publishMaster = 'no'
      healthChk = 'yes'
      healthChkCmd = 'curl -sS --fail -o /dev/null  http://localhost:8081/apidocs/ || exit 1'
    }
  }
}
