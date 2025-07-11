# Monitor de Marcas

## Tabela de conteúdo

- [Cliente](#cliente)
- [Parceiro](#parceiro)
- [Fontes de dados](#fontes-de-dados)
- [Descrição](#descrição)
- [Tipos de arquivos disponibilizados](#tipos-de-arquivos-disponibilizados)
- [Categorias de Pesquisas Kantar](#categorias-de-pesquisas-kantar)
  - [Categorias](#categorias)
- [Masterbrand](#masterbrand)
  - [Frequência de realização do Questionário](#frequência-de-realização-do-questionário)
  - [Frequência de envio dos arquivos Proprietários](#frequência-de-envio-dos-arquivos-proprietários)
  - [Marcas abordadas](#marcas-abordadas)
  - [Indicadores Proprietários Kantar](#indicadores-proprietários-kantar)
  - [Indicadores extraídos do Questionário](#indicadores-extraídos-do-questionário)
  - [Filtros extraídos do Questionário](#filtros-extraídos-do-questionário)
- [Arquitetura de Automação e Execução](#arquitetura-de-automação-e-execução)
  - [Lógica de execução](#lógica-de-execução)
  - [Nomenclatura dos recursos utilizados](#nomenclatura-dos-recursos-utilizados)

- [Dataproc Workflow Templates](#dataproc-workflow-templates)
  - [Workflow: questionario](#workflow-au029-marcas-masterbrand-questionario)
  - [Workflow: proprietarios](#workflow-au029-marcas-masterbrand-proprietarios)
- [Persistência](#persistência)
- [Indicadores do Questionário e Regras de Negócio](#indicadores-do-questionário-e-regras-de-negócio)
  - [Afinidade](#afinidade)
  - [Atende às Necessidades](#atende-às-necessidades)
  - [Confiança](#confiança)
  - [Consideração](#consideração)
  - [Diferente](#diferente)
  - [Dita Tendências](#dita-tendências)
  - [Familiaridade](#familiaridade)
  - [Imagem](#imagem-associação-absoluta)
  - [NPS](#nps)
  - [Preferência](#preferência)
  - [Fatores de Escolha](#fatores-de-escolha)
  - [Preço Percebido](#preço-percebido)
  - [Vale à Pena](#vale-à-pena)
- [Indicadores Proprietários Kantar](#indicadores-proprietários-kantar)
  - [Power e Dimensões](#power-e-dimensões)
  - [Premium](#premium)
  - [Contriubuição das Dimensões para o Power](#contriubuição-das-dimensões-para-o-power)
  - [BIP](#bip)
  - [Agrupamentos de Atributos BIP e Contribuição para o Power](#agrupamentos-de-atributos-bip-e-contribuição-para-o-power)
  - [Barreiras e Facilitadores](#barreiras-e-facilitadores)
  - [Média de Mercado do NPS para Conhecedores e Consumidores](#média-de-mercado-do-nps-para-conhecedores-e-consumidores)
- [Indicadores de Respostas Espontâneas](#indicadores-de-respostas-espontâneas)
- [Filtros](#filtros)
  - [Filtros Demográficos](#filtros-demográficos)
  - [Assinantes por plataforma](#assinantes-por-plataforma)
- [Dashboard](#dashboard)
  - [Indicadores Contemplados](#indicadores-contemplados)
  - [Estrutura](#estrutura)
  - [Métricas](#métricas)
  - [Granularidade de visualização](#granularidade-de-visualização)
  - [Filtros do dashboard](#filtros-do-dashboard)
- [Links e referências](#links-e-referências)





| Modificado por | Data       |
| :------------- | :--------- |
| Pedro Sales    | 10/10/2024 |
| Valter Lafuente   | 24/06/2025 |

## Cliente

Time de PESQUISA E CONHECIMENTO

Pontos focais:

- Raissa Paes, raissa.paes@g.globo
- Rafael Gussi, rafael.gussi@g.globo
- Glaucia Ferreira, glaucia.ferreira@g.globo

## Parceiro

Time de SOLUÇÕES DE MARCA, COMUNICAÇÃO E AUDIÊNCIA

Pontos focais:

- Roberto Roulin, roberto.moulin@g.globo
- Fernando Albino, fernando.albino@g.globo
- Tauan Jancso, tauan.jancso_stormgroup@prestador.globo

## Fontes de dados

Arquivos disponibilizados pelo time cliente em página do Sharepoint: [Sharepoint Monitor de Marcas](https://tvglobocorp.sharepoint.com/sites/solucoes_analiticas/monitor_marca/Forms/AllItems.aspx)

- Questionários de pesquisas Kantar preenchidos por pessoas físicas:
  Arquivos SPSS (formato .sav)
- Métricas geradas por cálculos proprietários Kantar: Arquivos Excel

## Descrição

Processo com o objetivo de automatizar a geração de métricas e visualizações de KPIs de Marcas para disponibilização para o time cliente, visando a evolução da solução integrada de Saúde de Marcas.

Os KPIs são obtidos através de pesquisas feitas com a sociedade, encomendadas com a Kantar, para entender a percepção com relação à marca Globo das pessoas que consomem nosso conteúdo, como nos avaliam e comparam a outras marcas, e que ações podemos tomar para melhorar.

Os arquivos referentes aos resultados dessas pesquisas, disponibilizados no Sharepoint, são processados em ambiente Google Cloud, gerando tabelas no BigQuery para cada um dos indicadores e filtros requisitados.

Essas tabelas são utilizadas para construção de dashboards no PowerBI referentes às métricas e cruzamentos de dados desejados, que são incorporados na aplicação desenvolvida pelo time parceiro, acessada pelo time cliente.

## Tipos de arquivos disponibilizados

Os arquivos disponibilizados pelo time cliente se enquadram em dois tipos distintos:

- Dados brutos de respostas de pessoas físicas a questionários formulados pela Kantar, a partir dos quais são extraídos uma série de indicadores e filtros conforme as regras de negócio passadas.
- Dados de indicadores/métricas específicos cuja fórmula de cálculo é propriedade da Kantar e são entregues já calculadas.

## Categorias de Pesquisas Kantar

Ambos os tipos de arquivos recebidos correspondem a formulários de pesquisa acordados com a Kantar, que se enquadram em 8 categorias, iniciados em Agosto de 2022 mas variando quanto às perguntas realizadas, frequência de realização do questionário e marcas abordadas.

##### Categorias

- Masterbrand
- Novelas
- Jornalismo
- Streaming
- Filmes e Séries
- Esportes
- Infantil
- Variedades

## Masterbrand

Para a categoria de **Masterbrand**, temos:

##### Frequência de realização do Questionário

- Relatório pontual inicial em Ago/2022 (considerado como Jul/2022 para fins de análise dos dados)
- Relatório pontual referente ao período Ago-Out/2022
- Relatórios pontuais em Nov/2022 e Dez/2022
- Relatórios trimestrais a partir de Jan/2023

##### Frequência de envio dos arquivos Proprietários

- Envios trimestrais para 2 indicadores
- Envios semestrais para 5 indicadores

##### Marcas abordadas

- Globo
- Netflix
- UOL
- Discovery
- Facebook
- Apple
- Twitter
- Amazon
- Spotify
- SBT
- Youtube
- HBO
- Record
- Google
- TikTok
- Disney
- Instagram

##### Indicadores Proprietários Kantar

- Power, Premium e Dimensões (semestral)
- Contriubuição das Dimensões para o Power (semestral)
- BIP (trimestral e semestral)
- Agrupamentos de Atributos BIP e Contribuição para o Power (semestral)
- Barreiras e Facilitadores (semestral)
- Média de Mercado do NPS para Conhecedores e Consumidores (trimestral)

##### Indicadores extraídos do Questionário

- Afinidade
- Atende às Necessidades
- Confiança
- Consideração
- Diferente
- Dita Tendências
- Familiaridade
- Imagem (associação absoluta)
- NPS
- Preferência
- Fatores de Escolha
- Preço Percebido
- Vale à Pena

##### Filtros extraídos do Questionário

- Familiaridade
- NPS
- Sexo
- Faixa Etária
- Classe Social
- Região
- Assinantes por Plataforma

## Arquitetura de Automação e Execução

A arquitetura de automação e execução do processo consiste em **um fluxo unificado com ramificação lógica** para: `questionario` e `proprietarios`.

Para a categoria **Masterbrand**, o processamento é realizado com base no projeto:

- `au029-marcas-masterbrand`

O fluxo é executado no ambiente **Google Cloud**, no projeto `gglobo-audiencia-hdg-prd`, com dois **Cloud Schedulers** distintos como gatilhos. Cada um dispara uma **Cloud Function única**, mas com parâmetros específicos:

- `"TIPO_PROCESSAMENTO": "QUESTIONARIO"`  
- `"TIPO_PROCESSAMENTO": "PROPRIETARIOS"`  

Ambos são agendados para execução diária às **8h (BRT)**, mas a lógica interna da **Cloud Function** garante o processamento apenas quando há novos arquivos disponíveis, respeitando a periodicidade de cada tipo de dado.

#### Lógica de execução:

1. Disparo agendado do **Cloud Scheduler**, com payload indicando o tipo de processamento (`TIPO_PROCESSAMENTO`)  
2. A **Cloud Function** executa de forma **independente para cada tipo de dado**, processando apenas o tipo especificado no payload (`questionario` ou `proprietarios`)  
3. A função autentica no SharePoint e verifica a existência de arquivos novos na pasta correspondente  
4. Os arquivos identificados como novos são salvos no **bucket LND** do Cloud Storage  
5. A função realiza a **detecção do tipo de período** (trimestral ou semestral) com base no nome do arquivo  
6. Para arquivos do tipo `proprietarios`, é realizada a **validação das abas obrigatórias** do Excel.  
   - Caso alguma aba esperada esteja ausente, o processo é **interrompido com erro** e um **e-mail é enviado** com as divergências encontradas  
7. A função gera um **arquivo auxiliar** no bucket de processamento com a lista de arquivos válidos a serem processados  
8. A **Cloud Function** dispara o **Workflow do Dataproc**, definindo os steps de execução com base no tipo informado  
9. O **Dataproc** lê o arquivo de controle com a lista de arquivos  
10. O **Dataproc** processa os arquivos localizados no bucket **LND**  
11. Os dados brutos são armazenados no bucket da camada **RAW**  
12. Cada indicador é persistido em tabelas específicas no **BigQuery**, na camada **PREP**



#### Nomenclatura dos recursos utilizados:

- **Cloud Scheduler**: `[SOLUCAO]-extract-sharepoint-cloudfunction`
- **Cloud Function**: `[SOLUCAO]-extract-sharepoint`
- **Dataproc Workflow Template**: `[SOLUCAO]-[TIPO]`
- **Buckets (Cloud Storage)**:
  - Camada **LND**: `[SOLUCAO]-lnd-fb23a`
  - Camada **RAW**: `[SOLUCAO]-raw-fb23a`
  - Bucket auxiliar: `[SOLUCAO]-dados-para-processamento`
- **Datasets (BigQuery)**:
  - Camada **RAW**: `raw_monitor_marcas_[CATEGORIA]`
  - Camada **PREP**: `prep_monitor_marcas_[CATEGORIA]`

## Dataproc Workflow Templates

#### Workflow: `au029-marcas-masterbrand-questionario`

**Etapas:**

- `load-questionario`  
  ➤ Esta etapa inclui uma **validação automática do arquivo da Kantar**, garantindo que ele esteja no formato e com o conteúdo esperado.  
  ➤ Caso o arquivo esteja com **divergências**, o processo é **interrompido com erro** e um **e-mail é enviado** com os detalhes das inconsistências encontradas.
- `transform-questionario-afinidade`
- `transform-questionario-diferente`
- `transform-questionario-dita-tendencias`
- `transform-questionario-necessidades`
- `transform-questionario-imagem`
- `transform-questionario-preferencia`
- `transform-questionario-confianca`
- `transform-questionario-familiaridade`
- `transform-questionario-consideracao`
- `transform-questionario-nps`
- `transform-questionario-filtro-demografico`
- `transform-questionario-filtro-consumo-meios`

---

#### Workflow: `au029-marcas-masterbrand-proprietarios`

**Etapas:**

- `load-metricas-bip`
- `load-metricas-peso-atributos`
- `load-metricas-facilitadores`
- `load-metricas-nps-mercado`
- `load-metricas-contribuicao-power`
- `load-metricas-premium`
- `load-metricas-power-dimensoes`

Onde:

- [SOLUCAO] -> Nome do fluxo específico do processo. (au029-marcas-masterbrand)
- [CATEGORIA] -> Nome da categoria. (masterbrand)
- [TIPO] -> Processamento a ser executado (`questionario` ou `proprietarios`).

![Arquitetura](documentacao/monitor_marcas-Arquitetura_MD.drawio.png)

## Persistência

A persistência dos dados também segue o mesmo princípio da arquitetura do processo, consistindo em ambientes separados para cada uma das categorias existentes.

- Os dados extraídos do sharepoint são armazenados sem alteração no Cloud Storage no bucket da camada LND.
- O arquivo do questionário no bucket LND passa por um processamento inicial para poder ser lido de forma tabular, e esse resultado é armazenado no Cloud Storage no formato Parquet, no respectivo bucket da camada RAW do fluxo.
- Os dados de questionário tabular no bucket da camada RAW também podem ser acessados através de uma tabela externa do BigQuery, que se encontra no respectivo dataset da camada RAW.
- Os dados do questionário tabular na camada RAW são processados para extração dos indicadores e filtros de interesse, e cada um deles é salvo em uma tabela nativa BigQuery individual no dataset da camada PREP
- Os dados de indicadories proprietários Kantar no bucket LND são processados separadamente e vão direto para as respetivas tabelas nativas BigQuery no dataset da camada PREP, cada um tendo a sua tabela individual.

![Persistência](documentacao/monitor_marcas-Persistencia.drawio.png)

## Indicadores do Questionário e Regras de Negócio

A partir das perguntas dos questionários realizados, são extraídos uma série de indicadores conforme regras de negócio estabelecidas pelo time cliente.

A maior parte desses indicadores corresponde a perguntas do questionário nas quais o respondente seleciona uma ou mais respostas de uma lista de opções já definidas, e cujas regras de negócio aplicadas podem ser compartilhadas e reproduzidas internamente.

##### Afinidade

Nomes alternativos: "Amor à Marca".

Tabela no BigQuery: "tb_afinidade"

Representa a percepção da população do quanto tem afinidade pela marca em questão, numa escala que vai de -3 a +3 conforme o critério:

- -3 -> "Eu odeio"
- 0 -> "Neutro"
- +3 -> "Eu amo"

Além das opções possíveis nessa escala, também é feita a classificação das respostas em outras categorias:

- "TOP2BOX" -> Corresponde a indivíduos que marcaram as opções 2 ou 3.
- "TOP2BOTTOM" -> Corresponde a indivíduos que marcaram as opções -3 ou -2.

Identificação da pergunta respectiva no questionário:

- Código -> "BE6"
- Texto -> "Como você se sente com relação a cada marca?"

Regra para identificação no arquivo SPSS:

- Código da pergunta começa com "BE6_BRAND"

##### Atende às Necessidades

Nomes alternativos: "Atende as minhas necessidades", "Necessidades".

Tabela no BigQuery: "tb_necessidades"

Representa a percepção da população do quanto a marca atende suas necessidades de entretenimento e informação, numa escala que vai de 1 a 7 conforme o critério:

- 1 -> "Não atende nenhuma das minhas necessidades"
- 7 -> "Atende muito bem as minhas necessidades"

Além das opções possíveis nessa escala, também é feita a classificação das respostas em outras categorias:

- "TOP2BOX" -> Corresponde a indivíduos que marcaram as opções 6 ou 7.
- "TOP2BOTTOM" -> Corresponde a indivíduos que marcaram as opções 1 ou 2.

Identificação da pergunta respectiva no questionário:

- Código -> "BP2"
- Texto -> "Arraste cada uma das marcas para a escala que indica o quanto cada marca atende as suas necessidades com relação a [TEXTO ESPECÍFICO POR CATEGORIA]."

Regra para identificação no arquivo SPSS:

- Código da pergunta começa com "BP2_BRAND"

##### Confiança

Tabela no BigQuery: "tb_confianca"

Representa a percepção da população do quanto confia na marca, numa escala que vai de 1 a 7 conforme o critério:

- 1 -> "Não confio nada"
- 7 -> "Confio totalmente"

Além das opções possíveis nessa escala, também é feita a classificação das respostas em outras categorias:

- "TOP2BOX" -> Corresponde a indivíduos que marcaram as opções 6 ou 7.
- "TOP2BOTTOM" -> Corresponde a indivíduos que marcaram as opções 1 ou 2.
- "Confio totalmente" -> Corresponde a indivíduos que marcaram a opção 7.
- "Neutro" -> Corresponde a indivíduos que marcaram as opções 3, 4, 5 ou 6.
- "Não confio em nada" -> Corresponde a indivíduos que marcaram as opções 1 ou 2.

Identificação da pergunta respectiva no questionário:

- Código -> "CONFIANCA"
- Texto -> "Arraste cada marca para a escala abaixo para indicar o quanto você confia nela."

Regra para identificação no arquivo SPSS:

- Código da pergunta começa com "CONFIANCA_BRAND"

##### Consideração

Tabela no BigQuery: "tb_consideracao"

Representa a percepção da população do quanto consideraria escolher a marca para consumo de entretenimento e informação, numa escala que vai de 1 a 4 conforme o critério:

- 1 -> "Seria a minha primeira escolha"
- 2 -> "Eu consideraria seriamente"
- 3 -> "Eu poderia considerar"
- 4 -> "Eu não consideraria"

Além das opções possíveis nessa escala, também é feita a classificação das respostas em outras categorias:

- "TOP2BOX" -> Corresponde a indivíduos que marcaram as opções 1 ou 2.
- "TOP2BOTTOM" -> Corresponde a indivíduos que marcaram as opções 3 ou 4.

Identificação da pergunta respectiva no questionário:

- Código -> "BE3"
- Texto -> "Qual a probabilidade de você escolher cada uma das seguintes marcas na sua próxima ESCOLHA de [TEXTO ESPECÍFICO POR CATEGORIA]?"

Regra para identificação no arquivo SPSS:

- Código da pergunta começa com "BE3_BRAND"

##### Diferente

Nomes alternativos: "Única".

Tabela no BigQuery: "tb_diferente"

Representa a percepção da população do quanto a marca em questão difere das demais, numa escala que vai de 1 a 7 conforme o critério:

- 1 -> "Exatamente a mesma coisa"
- 7 -> "Muito Diferente"

Além das opções possíveis nessa escala, também é feita a classificação das respostas em outras categorias:

- "TOP2BOX" -> Corresponde a indivíduos que marcaram as opções 6 ou 7.
- "TOP2BOTTOM" -> Corresponde a indivíduos que marcaram as opções 1 ou 2.

Identificação da pergunta respectiva no questionário:

- Código -> "BP1"
- Texto -> "Por favor arraste cada marca para o ponto da escala que melhor indica o quanto ela parece ser diferente das outras marcas de [TEXTO ESPECÍFICO POR CATEGORIA]."

Regra para identificação no arquivo SPSS:

- Código da pergunta começa com "BP1_BRAND"

##### Dita Tendências

Nomes alternativos: "Dita/Lança tendências", "Dinâmica", "Dinamismo".

Tabela no BigQuery: "tb_dita_tendencias"

Representa a percepção da população do quanto a marca em questão é responsável por lançar novas tendências em comparação com as demais, numa escala que vai de 1 a 7 conforme o critério:

- 1 -> "Segue/copia as outras"
- 7 -> "Dita/lança tendências"

Além das opções possíveis nessa escala, também é feita a classificação das respostas em outras categorias:

- "TOP2BOX" -> Corresponde a indivíduos que marcaram as opções 6 ou 7.
- "TOP2BOTTOM" -> Corresponde a indivíduos que marcaram as opções 1 ou 2.

Identificação da pergunta respectiva no questionário:

- Código -> "BP3"
- Texto -> "Arraste cada marca para a escala abaixo para indicar o quanto ela dita ou lança tendências."

Regra para identificação no arquivo SPSS:

- Código da pergunta começa com "BP3_BRAND"

##### Familiaridade

Nomes alternativos: "Funil de familiaridade", filtro "Consumidor".

Tabela no BigQuery: "tb_familiaridade"

Representa a percepção da população do quanto está familiarizada com a marca em questão, numa escala que vai de 1 a 6 conforme o critério:

- 1 -> "É a marca que eu consumo com maior frequência"
- 2 -> "É uma marca que eu consumo regularmente"
- 3 -> "Eu já consumi"
- 4 -> "Já vi ou ouvi falar muito nela, mas nunca consumi"
- 5 -> "Já vi ou ouvi falar um pouco nela, mas nunca consumi"
- 6 -> "Até hoje, nunca tinha visto ou ouvido falar nesta marca"

Além das opções possíveis nessa escala, também é feita a classificação das respostas em outras categorias:

- "Conhecedores" -> Corresponde a indivíduos que marcaram as opções 1, 2, 3, 4 ou 5.
- "Não Conhecedores" -> Corresponde a indivíduos que marcaram a opção 6.
- "Consumidores" -> Corresponde a indivíduos que marcaram as opções 1 ou 2.
- "Não Consumidores" -> Corresponde a indivíduos que marcaram as opções 4, 5 ou 6.
- "Consumidores Frequentes" -> Corresponde a indivíduos que marcaram a opção 1.
- "Consumidores Regulares" -> Corresponde a indivíduos que marcaram a opção 2.
- "Já Consumiu" -> Corresponde a indivíduos que marcaram as opções 1, 2 ou 3.
- "Abandonadores" -> Corresponde a indivíduos que marcaram a opção 3.
- "Conhecedores Não Consumidores" -> Corresponde a indivíduos que marcaram as opções 4 ou 5.

Identificação da pergunta respectiva no questionário:

- Código -> "BE2A"
- Texto -> "O quanto você está familiarizado(a) com cada uma destas marcas de entretenimento e informação?"

Regra para identificação no arquivo SPSS:

- Código da pergunta começa com "BE2A_BRAND"

##### Imagem (associação absoluta)

Nomes alternativos: "Imagem (%)", "Atributos de Imagem".

Tabela no BigQuery: "tb_imagem"

Representa a percepção da população sobre a visão que se tem da marca em questão, partindo de um grupo de opções de frases para as quais o respondente assinala que é relevante ("Sim") ou deixa a opção em branco ("Não"):

- "É uma marca que faz parte do meu dia a dia" (1)
- "É uma marca que é interativa com seu público" (2)
- "É uma marca que me permite aprender coisas novas" (3)
- "É uma marca que desperta emoções" (4)
- "É uma marca que conta boas histórias" (5)
- "É uma marca que está sempre lançando novidades" (6)
- "É uma marca que tem variedade que atende a diferentes gostos" (7)
- "É uma marca que entende profundamente o Brasil e os brasileiros" (8)
- "É uma marca que tem um time de profissionais diverso" (9)
- "É uma marca que é inspiração para outras marcas" (10)
- "É uma marca que está ao lado da população brasileira" (11)
- "É uma marca que investe para construir um futuro melhor" (12)

Essas opções de seleção também são classificadas em categorias conforme o seguinte critério:

- "Dia a dia/Aprendizado" -> Opções 1 e 3.
- "Conexão emocional" -> Opções 4 e 5.
- "Inspiração/Novidade" -> Opções 6 e 10.
- "Interatividade" -> Opção 2.
- "Variedade" -> Opções 7 e 9.
- "Brasilidade" -> Opções 8 e 11.
- "Futuro melhor" -> Opção 12.

Identificação da pergunta respectiva no questionário:

- Código -> "BD11"
- Texto -> "Pensando em marcas de [TEXTO ESPECÍFICO POR CATEGORIA], qual destas marcas acha que...? Selecione todas que se aplicam."

Regra para identificação no arquivo SPSS:

- Código da pergunta começa com "BD11\_" seguido de um dígito numérico

##### NPS

Nomes alternativos: "Recomendação".

Tabela no BigQuery: "tb_nps"

Representa a percepção da população sobre o quanto recomendariam a marca em questão para amigos e parentes, numa escala que vai de 0 a 10 conforme o critério:

- 0 -> "Nada provável"
- 10 -> "Muito provável"

Além das opções possíveis nessa escala, também é feita a classificação das respostas em outras categorias:

- "Detratores" -> Corresponde a indivíduos que marcaram as opções 0, 1, 2, 3, 4, 5 ou 6.
- "Neutros" -> Corresponde a indivíduos que marcaram as opções 7 ou 8.
- "Promotores" -> Corresponde a indivíduos que marcaram as opções 9 ou 10.

Identificação da pergunta respectiva no questionário:

- Código -> "NPS"
- Texto -> "Qual a probabilidade de você recomendar estas marcas para amigos ou parentes?"

Regra para identificação no arquivo SPSS:

- Código da pergunta começa com "NPS_BRAND"

##### Preferência

Tabela no BigQuery: "tb_preferencia"

Representa o posicionamento da população quanto à sua marca de preferência dentre as abordadas no questionário.

Identificação da pergunta respectiva no questionário:

- Código -> "PREF"
- Texto -> "Pensando nas marcas de [TEXTO ESPECÍFICO POR CATEGORIA], qual sua marca preferida?"

Regra para identificação no arquivo SPSS:

- Código da pergunta é igual a "PREF"

##### Fatores de Escolha

_Ainda não contemplado pelo dashboard._

Nomes alternativos: "Barreiras e facilitadores de mercado (associação absoluta)".

Representa a percepção da população sobre quais fatores influenciam na escolha da marca em questão, partindo de um grupo de opções de frases para as quais o respondente deve assinalar que é relevante ("Sim") ou deixar a opção em branco ("Não"):

- "Tem o melhor conteúdo para mim"
- "Tem o preço que eu estava disposto(a) a pagar"
- "Estava com preço promocional"
- "Vi um anúncio/ comercial"
- "Tem uma linguagem fácil de entender"
- "Oferece conteúdo sem anúncios/ sem propagandas"
- "É fácil de acessar"
- "Oferece a melhor experiência de uso"
- "Não consumi recentemente"
- "Nenhuma destas"
- "Não sei"

Identificação da pergunta respectiva no questionário:

- Código -> "ACT1"
- Texto -> "Quais frases abaixo descrevem fatores que influenciaram você na escolha das marcas de entretenimento e informação?"

Regra para identificação no arquivo SPSS:

- Código da pergunta começa com "ACT1_BRAND"

##### Preço Percebido

_Ainda não contemplado pelo dashboard._

Nomes alternativos: "Preço".

Representa a percepção da população do quanto ao preço da marca em questão com em comparação com as demais, numa escala que vai de 1 a 7 conforme o critério:

- 1 -> "Tem/teria o preço mais baixo"
- 7 -> "Tem/teria o preço mais alto"

Além das opções possíveis nessa escala, também é feita a classificação das respostas em outras categorias:

- "TOP2BOX" -> Corresponde a indivíduos que marcaram as opções 6 ou 7.
- "TOP2BOTTOM" -> Corresponde a indivíduos que marcaram as opções 1 ou 2.

Identificação da pergunta respectiva no questionário:

- Código -> "BP5"
- Texto -> "Destas marcas, algumas são pagas e outras gratuitas. Para esta pergunta, imagine que as gratuitas teriam um preço, ou seja, seriam pagas. Arraste cada marca para a escala para expressar sua opinião sobre o preço de cada uma delas."

Regra para identificação no arquivo SPSS:

- Código da pergunta começa com "BP5_BRAND"

##### Vale à Pena

_Ainda não contemplado pelo dashboard._

Representa a percepção da população do quanto consideram que a marca em questão vale em comparação com as demais, numa escala que vai de 1 a 3 conforme o critério:

- 1 -> "Vale menos que outras marcas"
- 2 -> "Vale o mesmo que outras marcas"
- 3 -> "Vale mais que outras marcas"

Identificação da pergunta respectiva no questionário:

- Código -> "BP6"
- Texto -> "Considerando o VALOR que essa empresa/marca entrega para você na categoria de entretenimento, quanto você acha que essa marca vale?"

Regra para identificação no arquivo SPSS:

- Código da pergunta começa com "BP6_BRAND"

## Indicadores Proprietários Kantar

Há outro tipo de indicadores que está vinculado a métricas cuja forma de cálculo é propriedade intelectual da Kantar, e consequentemente não é compartilhada com a Globo para ser reproduzida internamente a partir dos dados brutos dos questionários.

Para esses casos, são recebidos arquvios apartados com a informação já calculada para uso direto.

##### Power e Dimensões

Tabela no BigQuery: "tb_power_dimensoes"

Contém um índice de "Power", que representa uma metrificação da força da marca, além das três dimensões que compõem esse índice: "Diferenciação", "Significância" e "Saliência".

##### Premium

Tabela no BigQuery: "tb_premium"

Contém um índice de "Premium", composto a partir das mesmas dimensões do índice Power, que representa uma metrificação do quanto o valor percebido da marca suporta o seu preço atual.

##### Contriubuição das Dimensões para o Power

Tabela no BigQuery: "tb_contribuicao_power"

Contém o valor percentural da contribuição de cada uma das dimensões para o cálculo do índice de "Power".

##### BIP

Nomes alternativos: "Imagem/BIP (associação relativa)", "Brand Image Profile", "Imagem relativa".

Tabela no BigQuery: "tb_bip_relativo"

Parte da mesma lista de opções pré definidas do indicador de "Imagem (associação absoluta)", representando formas como o indivíduo pode enxergar a marca em questão.

Para cada opção, apresenta um índice representando o quanto a imagem geral da marca está associada a esse ponto (positiva ou negativamente), o que indica se este pode ser considerado uma força ou fraqueza relativa da marca.

##### Agrupamentos de Atributos BIP e Contribuição para o Power

_Ainda não contemplado pelo dashboard._

Tabela no BigQuery: "tb_peso_atributos"

Contém a categorização dos atributos de BIP em grupos, o peso de cada um dos atributos dentro do seu respectivo grupo, e o valor da contribuição percentual de cada um desses grupos para a formação do índice de "Power".

##### Barreiras e Facilitadores

_Ainda não contemplado pelo dashboard._

Tabela no BigQuery: "tb_barreiras_facilitadores"

Nomes alternativos: "Barreiras e facilitadores de mercado (índice)"

Parte da mesma lista de opções pré definidas do indicador de "Fatores de Escolha", representando fatores que levam o indivíduo a ecolher ou não a marca em questão como fonte de consumo de conteúdo.

Para cada fator, apresenta um índice percentual representando o quanto este influenciou (positiva ou negativamente) na escolha.

##### Média de Mercado do NPS para Conhecedores e Consumidores

_Ainda não contemplado pelo dashboard._

Tabela no BigQuery: "tb_nps_mercado"

Apresenta a média de mercado do índice de NPS, referente à probabilidade de se recomendar uma marca, nas visões de filtro de Familiaridade de "Consumidores" e "Conhecedores", para cada uma das marcas.

## Indicadores de Respostas Espontâneas

Existem ainda outros indicadores referentes a algumas perguntas específicas dos questionários nas quais o respondente passa uma resposta textual espontânea, ao contrário de marcar uma opção de pré definida.

Esses indicadores ainda não são contemplados pelo processo de carga de dados nem pelo dashboard, e seu entendimento ainda não está detalhado. Dentre eles, os que já foram mapeados são:

- Awareness espontaneo - Top of Mind
- Awareness espontaneo - Menções Totais
- Needs - Necessidades básicas da categoria - Menções totais
- Associação espontânea das marcas aos needs - Menções Totais
- Razões de não consumo
- Buzz - Ouviu falar alguma coisa sobre a Globo
- Buzz - O que ouviu falar
- Buzz - Concordância com o que ouviu falar

## Filtros

Para todos os indicadores, existe uma série de filtros que podem ser aplicados.

Parte deles corresponde a casos de indicadores que também são utilizados como filtros, aplicados a nível de cada combinação de respondente e marca, cujos valores possíveis são as opções de classificação da resposta dada pelo respondente para a marca em questão:

- NPS -> Opções de filtro: "Promotores", "Detratores", "Neutros"
- Familiaridade: -> Opções de filtro: "Conhecedores", "Consumidores", "Consumidores Frequentes", "Consumidores Regulares", "Já Consumiu", "Abandonadores", "Conhecedores Não Consumidores", "Não Consumidores", "Não Conhecedores"

Além deles, existem filtros que correspondem a perguntas específicas do questionário que não geram indicadores individuais, mas são utilizadas para gerar filtros cujos valores se aplicam diretamente ao respondente, sem fazer referência a nenhuma marca específica:

- Filtros Demográficos
- Assinantes por plataforma

Os dados para a aplicação desses filtros são armazenados em tabelas específicas no BigQuery.

##### Filtros Demográficos

Tabela no BigQuery: "tb_filtro_demografico"

Correspondem a quatro filtros distintos, cada um extraído de uma pergunta diferente do questionário:

- Sexo -> "Masculino", "Feminino"
- Região -> "Norte", "Nordeste", "Centro-Oeste", "Sudeste", "Sul"
- Faixa Etária -> "18-24", "25-34", "35-49", "50-65"
- Classe Social -> "A", "B", "A/B", "C"

##### Assinantes por plataforma

Tabela no BigQuery: "tb_filtro_consumo_meios"

Corresponde a uma pergunta do questionário na qual o respondente marca uma ou mais opções dentre uma série de possibilidades referentes a canais de consumo de conteúdo dos quais faz uso.

Até o mês de Outubro/2022, corresponde à pergunta referente a "Consumo de Meios". Entre Outubro/2022 e Junho/2023, a pergunta não constou no questionário e o filtro não é aplicável. A partir de Julho/2023, corresponde à pergunta referente a "STREAMING".

Nesse primeiro caso, a aplicação do filtro consiste apenas em listar os assinantes de TV paga, pela seleção dos usuários que marcaram a opção "**Canais de TV por assinatura**", independentemente de que outras opções tenham ou não sido assinaladas.

No segundo caso, a aplicação consiste em classificar os usuários em alguns grupos dependendo das opções que assinalaram, conforme abaixo:

- "Assinantes de streaming" -> Marcaram quaisquer opções válidas **exceto** "TV por assinatura como, clarotv, sky, vivotv, etc", "TV BOX com acesso à IPTV" e/ou "Não tenho acesso"
- "Assinantes de TV por assinatura" -> Marcaram a opção "TV por assinatura como, clarotv, sky, vivotv, etc"
- "Assinantes Pirata" -> Marcaram a opção "TV BOX com acesso à IPTV"
- "Não Assinantes" -> Marcaram a opção "Não tenho acesso"
- "Assinantes de Globoplay" -> Marcaram as opções "Globoplay versão paga simples" e/ou "Globoplay + Canais Ao Vivo"
- "Assinantes de Netflix" -> Marcaram a opção "Netflix"
- "Assinantes de Youtube Premium" -> Marcaram a opção "Youtube Premium"
- "Assinantes de Prime Video" -> Marcaram a opção "Amazon Prime Video"
- "Assinantes de Disney+" -> Marcaram a opção "Disney+"
- "Assinantes de Hbo Max" -> Marcaram a opção "Hbo Max"

## Dashboard

##### Indicadores Contemplados

A lista dos indicadores já contemplados e as tabelas no BigQuery com seus respectivos dados é a seguinte:

**Indicadores Proprietários**

- Power e Dimensões -> tb_power_dimensoes
- Premium -> tb_premium
- Contriubuição das Dimensões para o Power -> tb_contribuicao_power
- BIP -> tb_bip_relativo
- Agrupamentos de Atributos BIP e Contribuição para o Power -> tb_peso_atributos

**Indicadores extraídos do Questionário**

- Afinidade -> tb_afinidade
- Atende às Necessidades -> tb_necessidades
- Confiança -> tb_confianca
- Consideração -> tb_consideracao
- Diferente -> tb_diferente
- Dita Tendências -> tb_dita_tendencias
- Familiaridade -> tb_familiaridade
- Imagem (associação absoluta) -> tb_imagem
- NPS -> tb_nps
- Preferência -> tb_preferencia

##### Estrutura

Os indicadores de métricas proprietárias são extraídos já calculados, e já com os filtros possíveis, direto de suas respectivas tabelas.

Os demais indicadores, que vem do questionário, são importados para o PBI utilizando queries específicas que realizam um pré tratamento para os dados de cada um:

- Afinidade

```sql
WITH proc_class AS(
    SELECT
        * EXCEPT(cd_label, ds_label, ds_resposta, nm_indicador, ds_classificacao, ds_classificacao_aux, dt_processamento),
        CONCAT(cd_pessoa,dt_mes_inicio,dt_mes_fim) as id_pesquisa,
        CONCAT(cd_pessoa,dt_mes_inicio,dt_mes_fim,nm_marca) as id_pesquisa_marca,
        CASE WHEN ds_classificacao_aux = ds_resposta AND ARRAY_LENGTH(ds_classificacao) <= 1 THEN NULL
            ELSE ds_classificacao_aux
            END AS ds_classificacao_expl,
        CASE WHEN ds_resposta = '+3 Eu amo' THEN '7 Eu amo'
            WHEN ds_resposta = '+2' THEN '6'
            WHEN ds_resposta = '+1' THEN '5'
            WHEN ds_resposta = '0 Neutro' THEN '4 Neutro'
            WHEN ds_resposta = '-1' THEN '3'
            WHEN ds_resposta = '-2' THEN '2'
            WHEN ds_resposta = '-3 Eu odeio' THEN '1 Eu odeio'
            END AS ds_resposta,
        ds_resposta AS ds_resposta_orig,
        CASE WHEN nm_marca = "Amazon" then 1
            WHEN nm_marca = "Apple" then 2
            WHEN nm_marca = "Discovery" then 3
            WHEN nm_marca = "Disney" then 4
            WHEN nm_marca = "Facebook" then 5
            WHEN nm_marca = "Google" then 6
            WHEN nm_marca = "Globo" then 7
            WHEN nm_marca = "HBO" then 8
            WHEN nm_marca = "Instagram" then 9
            WHEN nm_marca = "Netflix" then 10
            WHEN nm_marca = "Record" then 11
            WHEN rtrim(ltrim(nm_marca)) = "SBT" then 12
            WHEN nm_marca = "Spotify" then 13
            WHEN nm_marca = "TikTok" then 14
            WHEN nm_marca = "Twitter" then 15
            WHEN nm_marca = "Youtube" then 16
            END id_marca
    FROM gglobo-audiencia-hdg-prd.prep_monitor_marcas_masterbrand.tb_afinidade,
        UNNEST(ds_classificacao) AS ds_classificacao_aux
)
SELECT * EXCEPT(ds_resposta_orig) FROM proc_class
WHERE (ds_classificacao_expl != ds_resposta_orig OR ds_classificacao_expl IS NULL)
```

- Confiança

```sql
WITH proc_regra AS(
    SELECT DISTINCT
        * EXCEPT(cd_label, ds_label, nm_indicador, ds_classificacao, ds_classificacao_aux, dt_processamento),
        CASE
            WHEN ds_classificacao_aux = ds_resposta AND ARRAY_LENGTH(ds_classificacao) <= 1 THEN NULL
            WHEN ds_classificacao_aux IN ("TOP2BOTTOM", "TOP2BOX") THEN NULL
            ELSE ds_classificacao_aux
            END AS ds_classificacao_expl
    FROM gglobo-audiencia-hdg-prd.prep_monitor_marcas_masterbrand.tb_confianca,
        UNNEST(ds_classificacao) AS ds_classificacao_aux
    WHERE (ds_classificacao_aux = ds_resposta AND ARRAY_LENGTH(ds_classificacao) <= 1)
        OR ds_classificacao_aux NOT IN ("TOP2BOTTOM", "TOP2BOX")
),
proc_top2 AS(
    SELECT DISTINCT
        * EXCEPT(cd_label, ds_label, nm_indicador, ds_classificacao, ds_classificacao_aux, dt_processamento),
        CASE
            WHEN ds_classificacao_aux = ds_resposta AND ARRAY_LENGTH(ds_classificacao) <= 1 THEN NULL
            WHEN ds_classificacao_aux NOT IN ("TOP2BOTTOM", "TOP2BOX") THEN NULL
            ELSE ds_classificacao_aux
            END AS ds_classificacao_expl_top2
    FROM gglobo-audiencia-hdg-prd.prep_monitor_marcas_masterbrand.tb_confianca,
        UNNEST(ds_classificacao) AS ds_classificacao_aux
    WHERE (ds_classificacao_aux = ds_resposta AND ARRAY_LENGTH(ds_classificacao) <= 1)
        OR ds_classificacao_aux IN ("TOP2BOTTOM", "TOP2BOX")
),
proc_class AS(
  SELECT * FROM proc_regra LEFT JOIN proc_top2
  USING(cd_pessoa,ds_periodo,dt_entrevista,nm_categoria,nm_marca,cd_resposta,dt_mes_inicio,dt_mes_fim,ds_onda,ds_resposta)
)
SELECT *,
    CONCAT(cd_pessoa,dt_mes_inicio,dt_mes_fim) as id_pesquisa,
    CONCAT(cd_pessoa,dt_mes_inicio,dt_mes_fim,nm_marca) as id_pesquisa_marca
FROM proc_class
WHERE (ds_classificacao_expl != ds_resposta OR ds_classificacao_expl IS NULL)
```

- Consideração

```sql
WITH proc_class AS(
    SELECT
        * EXCEPT(cd_label, ds_label, nm_indicador, ds_classificacao, ds_classificacao_aux, dt_processamento),
        CASE WHEN ds_classificacao_aux = ds_resposta AND ARRAY_LENGTH(ds_classificacao) <= 1 THEN NULL
            ELSE ds_classificacao_aux
            END AS ds_classificacao_expl
    FROM gglobo-audiencia-hdg-prd.prep_monitor_marcas_masterbrand.tb_consideracao,
        UNNEST(ds_classificacao) AS ds_classificacao_aux
)
SELECT
    *,
    CONCAT(cd_pessoa,dt_mes_inicio,dt_mes_fim) as id_pesquisa,
    CONCAT(cd_pessoa,dt_mes_inicio,dt_mes_fim,nm_marca) as id_pesquisa_marca
FROM proc_class
WHERE ds_classificacao_expl = "TOP2BOX" OR (cd_resposta IN (3,4) AND ds_classificacao_expl != "TOP2BOTTOM")
```

- Diferente

```sql
WITH proc_class AS(
    SELECT
        * EXCEPT(cd_label, ds_label, nm_indicador, ds_classificacao, ds_classificacao_aux, dt_processamento),
        CASE WHEN ds_classificacao_aux = ds_resposta AND ARRAY_LENGTH(ds_classificacao) <= 1 THEN NULL
            ELSE ds_classificacao_aux
            END AS ds_classificacao_expl
    FROM gglobo-audiencia-hdg-prd.prep_monitor_marcas_masterbrand.tb_diferente,
        UNNEST(ds_classificacao) AS ds_classificacao_aux
)
SELECT
    *,
    CONCAT(cd_pessoa,dt_mes_inicio,dt_mes_fim) as id_pesquisa,
    CONCAT(cd_pessoa,dt_mes_inicio,dt_mes_fim,nm_marca) as id_pesquisa_marca
FROM proc_class
WHERE (ds_classificacao_expl != ds_resposta OR ds_classificacao_expl IS NULL)
```

- Dita Tendências

```sql
WITH proc_class AS(
    SELECT
        * EXCEPT(cd_label, ds_label, nm_indicador, ds_classificacao, ds_classificacao_aux, dt_processamento),
        CASE WHEN ds_classificacao_aux = ds_resposta AND ARRAY_LENGTH(ds_classificacao) <= 1 THEN NULL
            ELSE ds_classificacao_aux
            END AS ds_classificacao_expl
    FROM gglobo-audiencia-hdg-prd.prep_monitor_marcas_masterbrand.tb_dita_tendencias,
        UNNEST(ds_classificacao) AS ds_classificacao_aux
)
SELECT
    *,
    CONCAT(cd_pessoa,dt_mes_inicio,dt_mes_fim) as id_pesquisa,
    CONCAT(cd_pessoa,dt_mes_inicio,dt_mes_fim,nm_marca) as id_pesquisa_marca
FROM proc_class
WHERE (ds_classificacao_expl != ds_resposta OR ds_classificacao_expl IS NULL)
```

- Familiaridade

```sql
WITH proc_class AS(
    SELECT
        * EXCEPT(cd_label, ds_label, nm_indicador, ds_classificacao, ds_classificacao_aux, dt_processamento),
        CONCAT(cd_pessoa,dt_mes_inicio,dt_mes_fim) as id_pesquisa,
        CONCAT(cd_pessoa,dt_mes_inicio,dt_mes_fim,nm_marca) as id_pesquisa_marca,
        CASE WHEN ds_classificacao_aux = ds_resposta AND ARRAY_LENGTH(ds_classificacao) <= 1 THEN NULL
            ELSE ds_classificacao_aux
            END AS ds_classificacao_expl
    FROM gglobo-audiencia-hdg-prd.prep_monitor_marcas_masterbrand.tb_familiaridade,
        UNNEST(ds_classificacao) AS ds_classificacao_aux
)
SELECT * FROM proc_class
WHERE (ds_classificacao_expl != ds_resposta OR ds_classificacao_expl IS NULL)
```

- Imagem

```sql
WITH proc_resp AS(
    SELECT
        * EXCEPT(cd_label, ds_label, cd_resposta, ds_resposta, nm_indicador, ds_classificacao, ds_classificacao_expl, dt_processamento),
        REGEXP_REPLACE(ds_classificacao_expl,"Que Que ","Que ") AS ds_resposta_proc
    FROM gglobo-audiencia-hdg-prd.prep_monitor_marcas_masterbrand.tb_imagem,
        UNNEST(ds_classificacao) AS ds_classificacao_expl
    WHERE ds_resposta = "Yes" AND UPPER(ds_classificacao_expl) LIKE "É UMA MARCA%"
)
SELECT
    * EXCEPT(ds_resposta_proc),
    REGEXP_REPLACE(ds_resposta_proc,"É Uma Marca Que ","") AS ds_resposta,
    CONCAT(cd_pessoa,dt_mes_inicio,dt_mes_fim) as id_pesquisa,
    CONCAT(cd_pessoa,dt_mes_inicio,dt_mes_fim,nm_marca) as id_pesquisa_marca,
FROM proc_resp
```

- Atende às Necessidades

```sql
WITH proc_class AS(
    SELECT
        * EXCEPT(cd_label, ds_label, nm_indicador, ds_classificacao, ds_classificacao_aux, dt_processamento),
        CASE WHEN ds_classificacao_aux = ds_resposta AND ARRAY_LENGTH(ds_classificacao) <= 1 THEN NULL
            ELSE ds_classificacao_aux
            END AS ds_classificacao_expl
    FROM gglobo-audiencia-hdg-prd.prep_monitor_marcas_masterbrand.tb_necessidades,
        UNNEST(ds_classificacao) AS ds_classificacao_aux
)
SELECT
    * ,
    CONCAT(cd_pessoa,dt_mes_inicio,dt_mes_fim) as id_pesquisa,
    CONCAT(cd_pessoa,dt_mes_inicio,dt_mes_fim,nm_marca) as id_pesquisa_marca
FROM proc_class
WHERE (ds_classificacao_expl != ds_resposta OR ds_classificacao_expl IS NULL)
```

- NPS

```sql
WITH proc_class AS(
    SELECT
        * EXCEPT(cd_label, ds_label, nm_indicador, ds_classificacao, ds_classificacao_aux, dt_processamento),
        CASE WHEN ds_classificacao_aux = ds_resposta AND ARRAY_LENGTH(ds_classificacao) <= 1 THEN NULL
            ELSE ds_classificacao_aux
            END AS ds_classificacao_expl
    FROM `gglobo-audiencia-hdg-prd.prep_monitor_marcas_masterbrand.tb_nps`,
        UNNEST(ds_classificacao) AS ds_classificacao_aux
)
SELECT
    *,
    CONCAT(cd_pessoa,dt_mes_inicio,dt_mes_fim) as id_pesquisa,
    CONCAT(cd_pessoa,dt_mes_inicio,dt_mes_fim,nm_marca) as id_pesquisa_marca,
    CONCAT(cast(extract(year from dt_mes_inicio) as STRING), format_date('%Q', dt_mes_fim)) as quarter_onda,
    format_date('%y%m', dt_entrevista) AS mes_ano_nr_entrevista,
    extract(year from dt_entrevista) as Ano,
    CASE WHEN extract(month from dt_entrevista) in (1, 2, 3, 4, 5, 6) THEN CONCAT('1S ', cast(extract(year from dt_entrevista) as string))
        ELSE CONCAT('2S ', cast(extract(year from dt_entrevista) as string))
        END Semestre,
    CONCAT(CAST(extract(quarter from dt_entrevista) AS STRING), "T ", cast(extract(year from dt_entrevista) as string)) as Trimestre,
    CASE WHEN DATE_DIFF(dt_mes_fim, dt_mes_inicio, MONTH) IN (0, 2) THEN ds_onda
        ELSE NULL
        END Trimestral,
    CASE WHEN ds_periodo = "Jul'2022" THEN ds_onda
        WHEN ds_periodo = "Ago'2022-Out'2022" THEN "Ago'2022-Dez'2022"
        WHEN ds_periodo = "Nov'2022" THEN "Ago'2022-Dez'2022"
        WHEN ds_periodo = "Dez'2022" THEN "Ago'2022-Dez'2022"
        WHEN extract(month from dt_entrevista) in (1, 2, 3, 4, 5, 6) THEN CONCAT("Jan'", CAST(EXTRACT(year from dt_entrevista) as STRING), "-Jun'", CAST(EXTRACT(year from dt_entrevista) as STRING))
        WHEN extract(month from dt_entrevista) in (7, 8, 9, 10, 11, 12) THEN CONCAT("Jul'", CAST(EXTRACT(year from dt_entrevista) as STRING), "-Dez'", CAST(EXTRACT(year from dt_entrevista) as STRING))
        END AS Semestral,
    CASE WHEN ds_periodo = "Jul'2022" THEN dt_mes_inicio
        WHEN ds_periodo = "Ago'2022-Out'2022" THEN DATE(CONCAT(cast(extract(year from dt_entrevista) AS STRING), "-08", "-01"))
        WHEN ds_periodo = "Nov'2022" THEN DATE(CONCAT(cast(extract(year from dt_entrevista) AS STRING), "-08", "-01"))
        WHEN ds_periodo = "Dez'2022" THEN DATE(CONCAT(cast(extract(year from dt_entrevista) AS STRING), "-08", "-01"))
        WHEN extract(month from dt_entrevista) in (1, 2, 3, 4, 5, 6) THEN DATE(CONCAT(cast(extract(year from dt_entrevista) AS STRING), "-01", "-01"))
        WHEN extract(month from dt_entrevista) in (7, 8, 9, 10, 11, 12) THEN DATE(CONCAT(cast(extract(year from dt_entrevista) AS STRING), "-07", "-02"))
        END as Ordem_Semestral,
    CASE WHEN ds_onda = "Jul'2022" THEN "20221"
        WHEN extract(month from dt_entrevista) in (1, 2, 3, 4, 5, 6) THEN CONCAT(CAST(EXTRACT(year from dt_entrevista) as STRING), "1")
        WHEN extract(month from dt_entrevista) in (7, 8, 9, 10, 11, 12) THEN CONCAT(CAST(EXTRACT(year from dt_entrevista) as STRING), "2")
        END nr_semestre_nps,
    CASE WHEN ds_onda = "Out'2022-Dez'2022" THEN "20225"
        ELSE CONCAT(cast(extract(year from dt_mes_inicio) as STRING), format_date('%Q', dt_mes_fim))
        END quarter_onda_nova,
    format_date('%h %y', dt_entrevista) AS mes_ano_entrevista,
FROM proc_class
WHERE (ds_classificacao_expl != ds_resposta OR ds_classificacao_expl IS NULL)
```

- Preferência

```sql
SELECT
    * EXCEPT(dt_processamento), CONCAT(cd_pessoa,dt_mes_inicio,dt_mes_fim) as id_pesquisa,
    CONCAT(cd_pessoa,dt_mes_inicio,dt_mes_fim,nm_marca) as id_pesquisa_marca
FROM gglobo-audiencia-hdg-prd.prep_monitor_marcas_masterbrand.tb_preferencia
```

```sql
WITH proc_class AS(
SELECT * EXCEPT(cd_label, ds_label, cd_resposta, nm_indicador, ds_classificacao, ds_classificacao_aux, dt_processamento),
CASE WHEN ds_classificacao_aux = ds_resposta AND ARRAY_LENGTH(ds_classificacao) <= 1 THEN NULL ELSE ds_classificacao_aux END AS ds_classificacao_expl
FROM gglobo-audiencia-hdg-prd.prep_monitor_marcas_masterbrand.tb_afinidade,
UNNEST(ds_classificacao) AS ds_classificacao_aux
)
SELECT * FROM proc_class
WHERE (ds_classificacao_expl != ds_resposta OR ds_classificacao_expl IS NULL)
```

Os indicadores sem query de extração listada são importados diretamente como estão nas suas tabelas de origem.

Existem ainda duas outras tabelas, referentes a filtros necessários para as visualizações, que também devem ser importadas, com queries diferentes:

- Filtros Demográficos

```sql
SELECT
    * EXCEPT(dt_processamento),
    CONCAT(cd_pessoa,dt_mes_inicio,dt_mes_fim) as id_pesquisa
FROM `gglobo-audiencia-hdg-prd.prep_monitor_marcas_masterbrand.tb_filtro_demografico`
```

- Filtros de Assinantes por Plataforma

```sql
WITH assinantes_antigo_base AS(
    SELECT DISTINCT
        nm_categoria,
        cd_pessoa,
        dt_mes_inicio,
        dt_mes_fim,
        "S" AS ds_classificacao_expl
    FROM `gglobo-audiencia-hdg-prd.prep_monitor_marcas_masterbrand.tb_filtro_consumo_meios`
    WHERE "Canais de TV por assinatura" IN UNNEST(ds_classificacao)
        AND dt_mes_inicio < "2023-01-01"
)
,assinantes_antigo AS(
    SELECT
        * EXCEPT(ds_consumo_meios,ds_classificacao,dt_processamento,ds_classificacao_expl),
        CONCAT(cd_pessoa,dt_mes_inicio,dt_mes_fim) as id_pesquisa,
        CASE WHEN ds_classificacao_expl IS NULL THEN "N"
            ELSE ds_classificacao_expl
            END AS ds_classificacao_expl
    FROM `gglobo-audiencia-hdg-prd.prep_monitor_marcas_masterbrand.tb_filtro_consumo_meios`
        LEFT JOIN assinantes_antigo_base
        USING (nm_categoria, cd_pessoa, dt_mes_inicio, dt_mes_fim)
    WHERE dt_mes_inicio < "2023-01-01"
)
,assinantes_novo AS(
    SELECT
        * EXCEPT(ds_consumo_meios,ds_classificacao,dt_processamento),
        CONCAT(cd_pessoa,dt_mes_inicio,dt_mes_fim) as id_pesquisa
    FROM `gglobo-audiencia-hdg-prd.prep_monitor_marcas_masterbrand.tb_filtro_consumo_meios`,
        UNNEST(ds_classificacao) AS ds_classificacao_expl
    WHERE dt_mes_inicio >= "2023-01-01"
)
SELECT
    cd_pessoa,
    ds_periodo,
    dt_entrevista,
    nm_categoria,
    dt_mes_inicio,
    dt_mes_fim,
    ds_onda,
    id_pesquisa,
    ds_classificacao_expl
FROM assinantes_antigo
UNION ALL
SELECT
    cd_pessoa,
    ds_periodo,
    dt_entrevista,
    nm_categoria,
    dt_mes_inicio,
    dt_mes_fim,
    ds_onda,
    id_pesquisa,
    ds_classificacao_expl
FROM assinantes_novo
```

Após essa importação, é feito o join da base de cada indicador com as seguintes outras tabelas, usando algumas colunas como chave:

- tb_filtro_demografico -> Colunas de join: "nm_categoria", "id_pesquisa"
- tb_filtro_consumo_meios -> Colunas de join: "nm_categoria", "id_pesquisa"
- tb_familiaridade -> Colunas de join: "nm_categoria", "id_pesquisa", "nm_marca"
- tb_nps -> Colunas de join: "nm_categoria", "id_pesquisa", "nm_marca"

A exceção para esse padrão são os indicadores de Familiaridade e NPS:

- Familiaridade
  - Não possui join com "tb_familiaridade", pois são os dados do próprio indicador
- NPS
  - Não possui join com "tb_nps", pois são os dados do próprio indicador

##### Métricas

Para todos os indicadores do questionário, a métrica calculada é o pecentual distinto de "cd_pessoa".

Essa contagem distinta considera as colunas do resultado da query de importação para o PBI que contêm a resposta dada para a pergunta pelo respondente e as possíveis classificações dessa resposta conforme as regras de negócio definidas. A coluna utilizada varia dependendo do indicador, podendo ser uma das seguintes:

- "ds_resposta"
- "ds_classificacao_expl"
- "ds_classificacao_aux"

Especificamente para o indicador de preferência, a métrica segue a mesma lógica, porém acessando apenas uma outra coluna:

- "cd_preferencia"

Além das colunas de respostas e classificações, é considerada também apenas uma dentre as seguintes colunas, dependendo do tipo de visualização selecionado:

- "nm_marca"
- "ds_onda"
- a extração do mês/ano de "dt_entrevista"

Além da métrica percentual de pessoas, apenas o indicador do NPS tem uma métrica adicional gerada no PBI, que é o Índice NPS.

O índice é calculado a partir da métrica percentual de pessoas para algumas classificações específicas de resposta, aplicando a seguinte regra:

> [% de pessoas classificadas como "Promotores"] - [% de pessoas classificadas como "Detratores"]

Com essa diferença sendo exibida em pontos percentuais.
Ex: 50% de Promotores - 30% de Detratores -> Índice NPS = 20

##### Granularidade de visualização

A nível da visualização, existem duas granularidades de tempo nas quais as métricas são calculadas:

- Por Onda -> usando a coluna "ds_onda"
- Por Mês/Trimestre/Semestre -> extraindo o mês e ano da coluna "dt_entrevista" e agrupando conforme a periodicidade desejada

Para cada uma dessas granularidades de cálculo, existem duas visões principais desejadas:

- Multi marca -> Passando "nm_marca" no eixo X e a Onda ou Mês como filtro
- Multi período -> Passando a Onda ou Mês no eixo X e "nm_marca" como filtro

##### Filtros

As visualizações da maioria dos indicadores (com exceção dos indicadores proprietários e alguns outros casos pontuais) apresentam também os seguintes filtros:

- "Consumidor" -> Corresponde ao indicador de Familiaridade. Conta com todas as opções da coluna "ds_classificacao_expl" da "tb_familiaridade" importada pela query especificada
- "NPS" -> Corresponde ao indicador de NPS. Conta com todas as opções da coluna "ds_classificacao_expl" da "tb_nps" importada pela query especificada
- "Assinantes por Plataforma" -> Conta com todas as opções da coluna "ds_classificacao_expl" da "tb_filtro_consumo_meios" importada pela query especificada
- "Sexo" -> Conta com todas as opções da coluna "ds_sexo" da "tb_filtro_demografico"
- "Região" -> Conta com todas as opções da coluna "ds_regiao" da "tb_filtro_demografico"
- "Idade" -> Conta com todas as opções da coluna "ds_faixa_etaria" da "tb_filtro_demografico"
- "Classe" -> Conta com todas as opções da coluna "ds_classe_social" da "tb_filtro_demografico"

As exceções para esse padrão são o indicador de Familiaridade (que não apresenta o filtro "Consumidor") e o indicador de NPS ("que não apresenta o filtro "NPS").

As visualizações do indicador NPS também não apresentam o filtro "Consumidor", pois o indicador é separado nas duas visões específicas de "NPS Consumidores" e "NPS Conhecedores", que já são aplicações desse filtro para valores específicos (respectivamene "Consumidores" e "Conhecedores").

## Links e referências

Aplicação desenvolvida pelo time parceiro:
[Monitor de Marcas](https://monitordemarcas.g.globo/)
