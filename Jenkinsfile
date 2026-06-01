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
                sh '''
                    for f in dags/*.py; do
                        [ -f "$f" ] && python3 -c "import ast; ast.parse(open('$f').read()); print('OK: $f')" || echo "Nenhuma DAG encontrada"
                    done
                '''
            }
        }
        stage('Validar Hooks') {
            steps {
                echo 'Validando Hooks...'
                sh '''
                    for f in hooks/*.py; do
                        [ -f "$f" ] && python3 -c "import ast; ast.parse(open('$f').read()); print('OK: $f')" || echo "Nenhum Hook encontrado"
                    done
                '''
            }
        }
        stage('Validar Operators') {
            steps {
                echo 'Validando Operators...'
                sh '''
                    for f in operators/*.py; do
                        [ -f "$f" ] && python3 -c "import ast; ast.parse(open('$f').read()); print('OK: $f')" || echo "Nenhum Operator encontrado"
                    done
                '''
            }
        }
        stage('Deploy') {
            steps {
                echo 'Copiando DAGs para o Airflow...'
                sh 'cp dags/*.py /var/jenkins_home/workspace/portfolio-dados/dags/ 2>/dev/null || echo "Nenhuma DAG para copiar ainda"'
            }
        }
    }
    post {
        success { echo '✅ Pipeline executado com sucesso!' }
        failure { echo '❌ Pipeline falhou! Verifique os erros acima.' }
    }
}
