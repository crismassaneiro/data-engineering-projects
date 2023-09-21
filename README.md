# projeto-de

Aqui vão API’s públicas que você pode usar para criar projetos interessantes:

- Exchange Rates Data API: Api que fornecesse informações de criptomoedas.

- OpenWeatherMap: API que fornece informações climáticas em tempo real de diversas cidades do mundo.

- NASA API: API da Agência Espacial Americana que fornece acesso a informações sobre a NASA, seus programas e missões espaciais.

- GitHub API: API da plataforma de gerenciamento de código fonte, que permite o acesso aos repositórios e informações de usuários.

- Spotify API: API que permite o acesso a informações de músicas, playlists e artistas da plataforma de streaming de música Spotify.

- PokeAPI: API que fornece informações sobre os personagens, habilidades, jogos e itens do universo Pokémon.

Agora como eu desenvolveria um projeto:
- Escolheria uma dessas APIs.
- Escreveria uma aplicação Python para extrair dados de alguns endpoints.
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
