# Event Process App

Este projeto visa na criação de um `event processor`, que garanta o processamento de todos os **eventos** criados por aplicações produtoras. O processamento feito por este event processor é receber, validar e salvar os eventos, para serem entregues aos devidos destinatários. A responsabilidade de entregar as messagens aos destinatários é de um outro componente, `sender` que poderá ser desenvolvida no futuro.

Este projeto disponibiliza `fake producers` presentes em `cmd/producers/main.go`. O objetivo desses fakes producers é validar a implementação do event processor.

## Requisitos

Os requisitos a serem atingidos pelo `event processor` são os seguintes:

1. Suportar `multi-tenant`, servindo para diferentes produtores, eventos e clientes;
1. Resilencia, sem perder nenhum evento gerado;
1. Disponibilidade, sempre online para processor os eventos;
1. Validação do payload/eventos recebidos;

## Infraestrutura

Este projeto utiliza o [Amazon SQS](https://aws.amazon.com/pt/sqs/) como message broker, "escutando" toda nova mensagem entregue na `file/queue`. Os fake producers se conectam ao SQS para registrar novos eventos.

Para a persistência das mensagens para serem posteriormente entregues aos clientes destinatários, é utilizado um banco de dados [PostgreSQL](https://www.postgresql.org/).

## Execução

Para executar localmente o projeto, todos os componentes listados na seção de infraestutura podem ser criados utilizando [Docker Containers](https://www.docker.com/). Para o Amazon SQS, é necessário a utilização do [LocalStack](https://github.com/localstack/localstack).

## Validação

## Considerações

### Decisões Tecnicas

### Tempo de Desenvolvimento

## For Doc

1. [AWS SQS Overview For Beginners](https://www.youtube.com/watch?v=CyYZ3adwboc)

## Endpoints

```bash
awslocal sqs send-message --queue-url http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/SQS_QUEUE --message-body "Hello World"
```

go test ./...
