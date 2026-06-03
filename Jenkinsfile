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
        stage('Deploy para Airflow') {
            steps {
                echo 'Copiando arquivos para o Airflow...'
                sh '''
                    AIRFLOW_DAGS=/home/zanca_awanne/portfolio-dados/dags

                    cp dags/*.py $AIRFLOW_DAGS/ && echo "DAGs copiadas!" || echo "Erro ao copiar DAGs"
                    cp hooks/*.py $AIRFLOW_DAGS/ && echo "Hooks copiados!" || echo "Erro ao copiar Hooks"
                    cp operators/*.py $AIRFLOW_DAGS/ && echo "Operators copiados!" || echo "Erro ao copiar Operators"
                '''
            }
        }
    }
    post {
        success { echo '✅ Pipeline executado com sucesso!' }
        failure { echo '❌ Pipeline falhou! Verifique os erros acima.' }
    }
}