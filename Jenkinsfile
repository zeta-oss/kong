pipeline {
    agent none
    options {
        retry(1)
        timeout(time: 2, unit: 'HOURS')
    }
    environment {
        UPDATE_CACHE = "true"
        DOCKER_CREDENTIALS = credentials('dockerhub')
        DOCKER_USERNAME = "${env.DOCKER_CREDENTIALS_USR}"
        DOCKER_PASSWORD = "${env.DOCKER_CREDENTIALS_PSW}"
        KONG_PACKAGE_NAME = "kong"
    }
    stages {
        stage('Build Kong') {
            agent {
                node {
                    label 'bionic'
                }
            }
            steps {
                sh 'curl https://ifconfig.me/'
            }
        }
    }
}
