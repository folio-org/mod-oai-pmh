buildMvn {
  publishModDescriptor = 'yes'
  mvnDeploy = 'no'

  doApiLint = true
  doApiDoc = true
  apiTypes = 'RAML'
  apiDirectories = 'ramls'

  buildNode = 'jenkins-agent-java11'
  doDocker = {
    buildJavaDocker {
      publishMaster = 'yes'
      healthChk = 'yes'
      healthChkCmd = 'curl -sS --fail -o /dev/null  http://localhost:8081/apidocs/ || exit 1'
    }
  }
}
