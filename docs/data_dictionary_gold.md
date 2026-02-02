# Camada Gold — Dicionário de Dados

A camada Gold segue o modelo estrela, com dados prontos para consumo analítico (BI / SQL), construída a partir dos datasets MCO e Tempo Real.

## Dimensões

### dim_data

Dimensão de calendário.

| Coluna | Descrição |
|--------|-----------|
| data_key | Chave da data no formato YYYYMMDD |
| data | Data (sem horário) |
| ano | Ano |
| mes | Mês |
| dia | Dia do mês |
| dia_semana | Nome do dia da semana |

Origem: datas do MCO (VIAGEM) e do Tempo Real (HR).

### dim_linha

Dimensão de linhas do sistema de transporte.

| Coluna | Descrição |
|--------|-----------|
| linha_key | Chave substituta da linha. |
| linha_numero | Identificador da linha (alfanumérico do MCO ou código numérico do Tempo Real convertido para string). |
| concessionaria_numero | Código da concessionária (quando disponível pelo MCO). |
| empresa_operadora | Código da empresa operadora (quando disponível pelo MCO). |

Observação: os códigos de linha do MCO (alfanuméricos) e do Tempo Real (numéricos) podem não ter correspondência direta.

### dim_concessionaria

Dimensão de consórcios/concessionárias.

| Coluna | Descrição |
|--------|-----------|
| concessionaria_key | Chave substituta da concessionária. |
| concessionaria_numero | Código do consórcio (801 Pampulha, 802 BHLeste, 803 Dez, 804 Dom Pedro II). |

### dim_empresa

Dimensão de empresas operadoras.

| Coluna | Descrição |
|--------|-----------|
| empresa_key | Chave substituta da empresa. |
| empresa_operadora | Código da empresa operadora conforme dicionário oficial. |

### dim_veiculo

Dimensão de veículos.

| Coluna | Descrição |
|--------|-----------|
| veiculo_key | Chave substituta do veículo. |
| veiculo_id | Número de ordem do veículo (BHTRANS). |

## Fatos

### fato_mco_viagem

Fato de viagens operacionais do MCO.
Granularidade: 1 registro por viagem.

| Coluna | Descrição |
|--------|-----------|
| data_key | FK para dim_data. |
| linha_key | FK para dim_linha. |
| concessionaria_key | FK para dim_concessionaria. |
| empresa_key | FK para dim_empresa. |
| veiculo_key | FK para dim_veiculo. |
| sublinha_numero | Número da sublinha. |
| ponto_controle_numero | Ponto de controle de origem. |
| hora_saida | Hora de saída da viagem. |
| hora_chegada | Hora de chegada da viagem. |
| extensao_viagem | Extensão da viagem (metros). |
| total_usuarios_viagem | Total de usuários transportados. |
| indicador_ocorrencia | Indicador de interrupção da viagem. |
| indicador_falha_mecanica | Indicador de falha mecânica (S/N). |
| indicador_evento_inseguro | Indicador de evento inseguro (S/N). |

### fato_tempo_real_evento

Fato de eventos de posição em tempo real.
Granularidade: 1 registro por evento.

| Coluna | Descrição |
|--------|-----------|
| data_key | FK para dim_data. |
| linha_key | FK para dim_linha. |
| veiculo_key | FK para dim_veiculo. |
| codigo_evento | Código do evento (ex.: 105 = posição). |
| data_hora | Data e hora do evento. |
| latitude | Latitude (WGS84). |
| longitude | Longitude (WGS84). |
| velocidade_instantanea | Velocidade do veículo. |
| sentido_veiculo | Sentido da viagem (1 ida, 2 volta). |
| distancia_percorrida | Distância percorrida. |

