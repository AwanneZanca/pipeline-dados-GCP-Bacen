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
                    # Copia DAGs
                    if cp dags/*.py /home/zanca_awanne/portfolio-dados/dags/ 2>/dev/null; then
                        echo "DAGs copiadas com sucesso!"
                    else
                        echo "Nenhuma DAG para copiar"
                    fi

                    # Copia Hooks
                    if cp hooks/*.py /home/zanca_awanne/portfolio-dados/dags/ 2>/dev/null; then
                        echo "Hooks copiados com sucesso!"
                    else
                        echo "Nenhum Hook para copiar"
                    fi

                    # Copia Operators
                    if cp operators/*.py /home/zanca_awanne/portfolio-dados/dags/ 2>/dev/null; then
                        echo "Operators copiados com sucesso!"
                    else
                        echo "Nenhum Operator para copiar"
                    fi
                '''
            }
        }
    }
    post {
        success { echo '✅ Pipeline executado com sucesso!' }
        failure { echo '❌ Pipeline falhou! Verifique os erros acima.' }
    }
}