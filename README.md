# Hurb: Desafio Data Engineer
**Autor: Matheus Fernandes Moreno**

Este repositório contém o código-fonte da solução para o desafio para a vaga de
Data Engineer na Hurb. O projeto consistiu no desenvolvimento de um sistema
capaz de agregar informações e dados sobre os estados brasileiros durante o
início da pandemia de Covid-19, a partir de dois arquivos CSV.

O sistema foi implementado em Python e utiliza-se do Apache Beam. Esta ferramenta
é particularmente útil na resolução de problemas que podem ser divididos em
subproblemas e então paralelizados, gerando assim um pipeline de processamento.

(Aqui, falar um pouco mais por que usamos o Apache Beam.)

## O pipeline

O fluxo foi projetado considerando simplicidade e robustez. Considerando que o CSV
com informações sobre as unidades federativas não precisa de nenhum tipo de agregação
ou filtragem, este foi importado separadamente, como uma tabela adicional para incrementar
os dados principais; a única alteração feita às informações desse arquivo foi a renomeação
das colunas. Deste modo, a `PCollection` processada pelo Beam consiste das linhas do CSV
com dados da pandemia.

As etapas do fluxo principal são:

1. A `PCollection` é gerada a partir de um gerador de dicionários, sendo cada um deles
associado a uma linha do arquivo ingerido;
2. As linhas não relativas às unidades federativas são excluídas da coleção;
3. Os dados são agrupados por código da unidade federativa, região e estado. Em
seguida, são executadas duas agregações para calcular o total de casos e de óbitos.
O código da unidade federativa é único por estado, mas precisamos agrupar os dados
por estes três valores pois a ação de agregação preserva apenas os campos de
agrupamento e o resultado da agregação;
4. Os dados são "incrementados" com informações sobre os estados;
5. Os campos relevantes são filtrados e renomeados.

A partir deste momento, o fluxo se divide em dois subfluxos: um responsável por
gerar o arquivo CSV, e outro por gerar o arquivo JSON. O primeiro consiste em:

1. Formatar os dados em uma string, representando o arquivo CSV por completo.
2. Escrevendo a string no arquivo de saída, com extensão `.csv`.

O subfluxo do JSON é análogo.

## Uso

O script pode ser executado em sua máquina local, ou num ambiente Docker. A
primeira opção é recomendada, visto que o sistema demorou consideravelmente mais
para rodar no ambiente dockerizado (5min na máquina local do autor, 45min no ambiente
Docker, com 6 núcleos e 6GB de memória para o contâiner); porém, a segunda opção é
obviamente importante por motivos de portabilidade.

### Localmente

O sistema é simplesmente um script Python que depende da biblioteca do Apache Beam.
Assim, após a instalação da dependência,

```
# A linha abaixo, ou apenas 'pip install apache-beam'
pip install -r requirements.txt
```

O script pode ser executado com

```
python covid_aggregator.py [-i INPUT_FILE] [-o OUTPUTS_PREFIX] [-s STATES_FILE]
```

Os argumentos são opcionais e definem, respectivamente, o arquivo de entrada,
o prefixo para os arquivos de saída, e o arquivo com informações adicionais sobre
as unidades federativas.

### Ambiente Docker

Para rodar a máquina num ambiente dockerizado, recomenda-se o uso do Docker Compose,

```
# A partir da versão 3.4.0, o Compose faz parte do próprio Docker
docker compose up
```

Os argumentos de configuração podem ser passados introduzindo o campo `command`
no arquivo do Docker Compose, como comentado em `docker-compose.py`.
