pipeline {
    agent any

    stages {
        stage('Clonar o repositório') {
            steps {
                git branch: 'QualidadePosVenda',
                    url: 'https://github.com/JoaoParvi/RPA_QualidadeHyundai.git'
            }
        }

        stage('Instalar dependências') {
            steps {
                bat '"C:\\Users\\adm.luiz.vinicius\\AppData\\Local\\Programs\\Python\\Python312\\Scripts\\pip.exe" install -r requirements.txt'
            }
        }

        stage('Executar script Python') {
            steps {
                bat '''
            C:\\Users\\adm.luiz.vinicius\\AppData\\Local\\Programs\\Python\\Python312\\python.exe QualidadeHyundai.py > script_log.txt 2>&1
            type script_log.txt
        '''
    }
}
    }

    post {
        success {
            script {
                def log = readFile('script_log.txt')
                emailext(
                    subject: "SUCESSO: ${env.JOB_NAME} #${env.BUILD_NUMBER}",
                    body: """
                        <p>O job <b>${env.JOB_NAME}</b> finalizou com <b>sucesso</b>.</p>
                        <p><a href='${env.BUILD_URL}'>Ver detalhes no Jenkins</a></p>
                        <pre>${log}</pre>
                    """,
                    mimeType: 'text/html',
                    to: 'rpabiparvi@gmail.com',
                    attachmentsPattern: 'script_log.txt'
                )
            }
        }

        failure {
            script {
                def log = readFile('script_log.txt')
                emailext(
                    subject: "FALHA: ${env.JOB_NAME} #${env.BUILD_NUMBER}",
                    body: """
                        <p>O job <b>${env.JOB_NAME}</b> falhou.</p>
                        <p><a href='${env.BUILD_URL}'>Ver detalhes no Jenkins</a></p>
                        <pre>${log}</pre>
                    """,
                    mimeType: 'text/html',
                    to: 'rpabiparvi@gmail.com',
                    attachmentsPattern: 'script_log.txt'
                )
            }
        }
    }
}
