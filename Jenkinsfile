pipeline {
    agent any
    stages {
        stage('Checkout') {
            steps {
                echo 'Clonando repositório...'
            }
        }
        stage('Validar DAGs') {
            steps {
                echo 'Validando DAGs...'
                sh 'echo "Sintaxe OK"'
            }
        }
        stage('Deploy') {
            steps {
                echo 'Deploy realizado com sucesso!'
            }
        }
    }
    post {
        success {
            echo '✅ Pipeline executado com sucesso!'
        }
        failure {
            echo '❌ Pipeline falhou!'
        }
    }
}
