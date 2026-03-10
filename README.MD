# Contexto do projeto

Esse data lakehouse foi criado para fornecer dados pro meu projeto de análise, no qual investiga como a criação de conteúdo está fazendo jogos indies terem um crescimento exponencial. Abaixo você verá alguns problemas que enfrentei ao longo da criação ao mesmo tempo que fornece detalhes sobre ela.

![Análise dos jogos indies](https://app.powerbi.com/view?r=eyJrIjoiYWExNmJhZDUtOTk2Zi00NGI0LThkYmEtYTM2MmIxYzhhM2QxIiwidCI6IjllOTQ2M2EwLWMyNTEtNGNmNi04NDgxLWJlNzRmNWY2YzQ0YiJ9&pageName=291505a107e08583c527)

## Detalhes e Problemas Resolvidos

### Orquestração

#### Problema: dados com timestamp diferente

Se os dados extraídos da twitch e steam vierem com timestamps diferentes, será necessário mais processamento(mais custo) só para ajustar os tempos. 

#### Solução:

Tasks e scripts do DAG agora recebem um único timestamp pros dados em batch, facilitando a deduplicação na silver e joins com hash na gold.

```python
ingestion_timestamp_utc = datetime.now().isoformat()
TASK 1 get_twitch_data(ingestion_timestamp_utc, ...)
TASK 2 get_steam_data(ingestion_timestamp_utc, ...)
```

---

### Ingestão, Idempotência e Custo

#### Problema: full load vs incremental

O monitoramento de apenas 5 jogos gera 133K registros por dia. Se o pipeline nao tiver ingestão incremental e nem medidas de Data Skip, o custo operacional vai aumentar a cada dia.

#### Solução:

- Ingestao incremental e checkpoints em todas as camadas, processando apenas dados novos.
- Idempotência com incremental merge e liquid clustering nas camadas silver e gold.

---

### Transformação e escalabilidade

#### Problema: dados duplicados:

Os dados extraídos da Twich podem vir com dados duplicados devido sua estrutura dinâmica.

#### Solução:

- Entender a forma dos dados duplicados: stream_id com o mesmo timestamp(padronizado na orquestração)
- deduplicação dos dados utilizando QUALIFY.
- criação de hash na camada gold para facilitar o join na fato.
- camada silver e gold com liquid clustering e delta lake

---

#### Testes

- Data Contracts
- Integridade do dag
- Pytest

#### Manutenção

- Versionamento / CI-CD
- Ci-airflow (verifica a sintaxe do dag)
- Ci-dbt (verifica a sintaxe das querys e testa a lógica de deduplicação)
- OPTIMIZE (job databricks)

```text
Guia das Pastas/
├── .github/
│   └── workflows # ci/cd
├── airflow_docker/ 
│   ├── dags/
│   │   │── scripts/ # auth, extractors, transform e loader
│   └── tests/ # teste de integridade do dag
├── databricks/ # ingestão incremental na camada bronze
├── dbt/
│   ├── models/
│   │   ├── gold/ # fato, gold_steam_daily, gold_twitch_daily
│   │   ├── silver/ # deduplicação e agregação das lives
│   │   └── staging/ # padronização das colunas
```

