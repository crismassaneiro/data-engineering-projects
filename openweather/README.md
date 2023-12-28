# projeto-de
--------------------------------------------------------------------------------

1 - Escolher uma API para puxar dados:

Usaremos a API da Open Weather Map:  https://openweathermap.org/guide

O que você deve fazer aqui: você deve ir no site e ler a documentação. Você deve achar a área que ensina como puxar os dados, os famosos "endpoints".

Mas que caralhos é um endpoint: Claro, vou explicar de forma simples o que são os endpoints em relação a APIs:

Os "endpoints" são como janelas de atendimento em uma loja, onde você vai para obter um serviço específico. Em uma API, um endpoint é um ponto de entrada ou um URL específico que você solicita para obter informações ou executar ações.

Pense em uma API como uma grande loja de informações ou serviços. Cada serviço ou conjunto de informações que você deseja acessar tem seu próprio "balcão de atendimento" designado, que é o endpoint. Por exemplo, se você está usando uma API de previsão do tempo, pode haver um endpoint para obter a previsão atual, outro endpoint para obter a previsão de cinco dias e assim por diante.

Quando você faz uma solicitação para um endpoint específico, está basicamente dizendo à API o que deseja e para onde deseja ir dentro da "loja". A API, em seguida, fornece os dados ou realiza a ação correspondente e retorna uma resposta para você.

Em resumo, os endpoints são como portas de entrada para acessar informações ou funcionalidades específicas em uma API, permitindo que você interaja com ela de maneira controlada e organizada. Cada endpoint representa uma tarefa ou recurso diferente que a API pode fornecer.

--------------------------------------------------------------------------------

- Escreveria uma aplicação Python para extrair dados de alguns endpoints.














-------------------------------------------------------------------------------
- Escreveria uma aplicação Python para extrair dados incrementais dos endpoints escolhidos.
- Persistiria os dados em formato json em um data lake, por exemplo s3 na AWS.
- Escreveria uma aplicação para transformar os dados
- Unificando todos os jsons para um arquivo delta da camanda raw e deixando isso simples em uma única tabela para ser consumida, por exemplo.
- Particionando os dados por data.
- Criando campos adicionais como por exemplo: ingestion_time (timestamp).
- Implementaria um orquestrador com o Airflow para automatizar todas essas etapas.
- Documentaria tudo com muito detalhe no Github.

Esse é apenas um exemplo de projeto.

Eu gosto muito de sugerir usar python para fazer o que ferramentas fazem ..

É a melhor forma de você mostrar que você sabe escrever pipelines..
