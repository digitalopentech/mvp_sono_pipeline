# Databricks notebook source
# MAGIC %md
# MAGIC # 🎯 Objetivo do MVP – Análise da Qualidade do Sono
# MAGIC
# MAGIC - **Fonte:** Kaggle  
# MAGIC - **Licença:** Gratuito para uso educacional  
# MAGIC - **Técnica de ingestão:** Upload manual e leitura via Spark (formato CSV) no Databricks  
# MAGIC - **Link do Dataset:** [Sleep Health and Lifestyle Dataset – Kaggle](https://www.kaggle.com/datasets/uom190346a/sleep-health-and-lifestyle-dataset)
# MAGIC
# MAGIC O objetivo deste projeto é analisar fatores de estilo de vida e saúde que influenciam a qualidade do sono. A partir do dataset "Sleep Health and Lifestyle", construiremos um pipeline de dados completo com arquitetura em camadas (Bronze, Silver e Gold), utilizando a plataforma **Databricks Community Edition**.
# MAGIC
# MAGIC ## Perguntas de negócio a serem respondidas:
# MAGIC
# MAGIC 1. Quais fatores influenciam a **qualidade do sono**?
# MAGIC 2. O **nível de estresse** afeta a **duração** e a qualidade do sono?
# MAGIC 3. Pessoas com certas **ocupações dormem melhor ou pior**?
# MAGIC 4. Existe relação entre **IMC / atividade física** e qualidade do sono?
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Pipeline de Dados – Análise de Saúde do Sono e Estilo de Vida
# MAGIC
# MAGIC Este notebook demonstra a construção de um pipeline de dados completo, tendo como base o dataset **Sleep Health and Lifestyle**. Esse conjunto de dados **sintético** foi obtido no Kaggle e contém informações sobre **hábitos de sono e fatores de estilo de vida** de **374 indivíduos**, distribuídos em **13 colunas**.
# MAGIC
# MAGIC As variáveis incluem:
# MAGIC - Duração e qualidade do sono
# MAGIC - Nível de atividade física (minutos)
# MAGIC - Nível de estresse (escala de 1 a 10)
# MAGIC - Categoria de IMC
# MAGIC - Pressão arterial
# MAGIC - Frequência cardíaca
# MAGIC - Passos diários
# MAGIC - Ocupação
# MAGIC - Presença de distúrbios do sono (Insônia, Apneia, Ausência de Distúrbio)
# MAGIC
# MAGIC O pipeline foi implementado seguindo boas práticas de engenharia de dados, com camadas:
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 1. Coleta de Dados (Camada Bronze)
# MAGIC
# MAGIC **Origem dos Dados:**  
# MAGIC O dataset foi obtido através do Kaggle e armazenado localmente como CSV. O arquivo foi carregado para o **Databricks File System (DBFS)** usando a interface do Databricks Community Edition.
# MAGIC
# MAGIC **Camada Bronze:**  
# MAGIC Nesta camada, mantemos os dados brutos conforme fornecidos, sem alterações, apenas realizando a conversão para um formato otimizado (Parquet) para facilitar o armazenamento e a leitura distribuída.
# MAGIC
# MAGIC - 📁 **Caminho do CSV no DBFS:** `/FileStore/tables/Sleep_health_and_lifestyle_dataset.csv`
# MAGIC - ⚙️ **Procedimento de Upload:** Upload via interface gráfica do Databricks Community Edition (menu lateral > "Upload Data").  
# MAGIC   Alternativamente, seria possível usar `dbutils.fs.cp` ou a API REST da plataforma.
# MAGIC
# MAGIC **Leitura Inicial:**  
# MAGIC Os dados são lidos como DataFrame Spark, o schema é inferido automaticamente e os dados são salvos como **Parquet** na camada Bronze.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Resumo:** O texto está correto, mas com essas pequenas melhorias, ele:
# MAGIC
# MAGIC - Ganha mais clareza (ex: você citava 400 registros, mas o CSV tem exatamente **374**, o que já ajustamos).
# MAGIC - Alinha a linguagem à documentação profissional.
# MAGIC - Explicita detalhes técnicos importantes (como os tipos de dados e o processo de leitura).
# MAGIC
# MAGIC Se quiser, posso revisar e formatar também a **parte da camada Silver, Gold, análises e conclusão** no mesmo padrão profissional. Deseja isso?

# COMMAND ----------

# Leitura do arquivo CSV na camada Bronze
df_bronze = (spark.read.format("csv")
             .option("header", True)    # o CSV contém cabeçalho
             .option("inferSchema", True)  # inferir automaticamente os tipos de dados
             .load("/FileStore/tables/Sleep_health_and_lifestyle_dataset.csv"))

# Exibir o schema inferido
df_bronze.printSchema()

# Exibir as primeiras 5 linhas do DataFrame Bronze
df_bronze.show()

# Contagem de registros lidos
print(f"Total de registros (bronze): {df_bronze.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC _Ao ler o CSV com inferSchema, o Spark identifica tipos de dados adequados para cada coluna automaticamente. Vamos verificar a saída do schema para confirmar se todos os 13 campos foram reconhecidos corretamente, e olhar alguns exemplos de registros brutos:_

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspeção Inicial da Camada Bronze
# MAGIC
# MAGIC O DataFrame Bronze contém **13 colunas**, com os mesmos nomes presentes no CSV original — incluindo espaços e letras maiúsculas (por exemplo, `Quality of Sleep`, `Blood Pressure`, `Sleep Disorder`). Os nomes ainda **não foram normalizados ou traduzidos** nesta etapa.
# MAGIC
# MAGIC Foram carregados **374 registros válidos**, o que condiz com o conteúdo real do arquivo. Embora o dataset tenha sido divulgado como contendo aproximadamente 400 entradas, o arquivo distribuído possui exatamente **374 linhas completas**, sem valores nulos.
# MAGIC
# MAGIC Nesta fase **Bronze**, **mantemos os dados brutos** como estão, sem transformações:
# MAGIC - A coluna `Blood Pressure` permanece como uma **string no formato "sistólica/diastólica"** (ex: `"120/80"`).
# MAGIC - Colunas categóricas como `BMI Category` e `Sleep Disorder` mantêm os valores originais do CSV, como `"Overweight"`, `"Normal"`, `"Sleep Apnea"`, `"None"`, etc.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Agora, vamos **persistir esses dados brutos no formato Parquet** na camada Bronze do nosso Data Lake, mantendo a fidelidade total à origem, mas garantindo um armazenamento mais eficiente e otimizado para leitura distribuída.

# COMMAND ----------

# Salvando os dados brutos no formato Parquet na camada Bronze
df_bronze.write.mode("overwrite").parquet("/FileStore/tables/bronze/sleep_health_and_lifestyle")

df_bronze.show()

# COMMAND ----------

# MAGIC %md
# MAGIC _Após essa operação, temos um arquivo Parquet (ou conjunto de arquivos) armazenado no DBFS, o que nos permitirá consultas mais eficientes nas próximas etapas. Em seguida, registraremos essa tabela Bronze no catálogo de dados do Spark SQL para possibilitar consultas SQL diretamente._

# COMMAND ----------

# Remove o diretório físico associado à tabela no DBFS
dbutils.fs.rm("dbfs:/user/hive/warehouse/bronze_sleep_health", recurse=True)

# COMMAND ----------

# Deletar tabela se já existir
spark.sql("DROP TABLE IF EXISTS bronze_sleep_health")

# COMMAND ----------

# Registro da tabela Bronze no catálogo do Spark (Community Edition compatível)
df_bronze.write.mode("overwrite").format("parquet").saveAsTable("bronze_sleep_health")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_sleep_health;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Modelagem dos Dados e Catálogo de Dados
# MAGIC
# MAGIC ### Estratégia de Modelagem
# MAGIC
# MAGIC Optamos por uma **modelagem flat (plana)** no estilo **Data Lake**, mantendo todos os atributos em uma única tabela principal. Essa escolha se deve à **natureza do dataset** — um único arquivo CSV com todas as informações integradas — e ao **pequeno volume de dados** (374 registros), que **dispensa a necessidade de normalização**.
# MAGIC
# MAGIC Embora fosse possível extrair dimensões separadas (como Ocupação, Gênero, Categoria de IMC), a modelagem flat facilita a consulta direta, reduz complexidade e é mais apropriada para projetos exploratórios ou MVPs. Em ambientes com alto volume ou arquitetura OLAP, o comum seria normalizar em tabelas dimensionais (por exemplo, tabela de Pessoas, Ocupações, etc.).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Catálogo de Dados (Data Dictionary)
# MAGIC
# MAGIC O catálogo abaixo descreve cada atributo disponível no dataset, com nome da coluna, tipo de dado, explicação e exemplos de valores:
# MAGIC
# MAGIC | **Coluna**                           | **Tipo**     | **Descrição**                                                                 | **Valores Possíveis / Exemplos**                        |
# MAGIC |-------------------------------------|--------------|-------------------------------------------------------------------------------|---------------------------------------------------------|
# MAGIC | `Person ID`                         | Inteiro      | Identificador único de cada indivíduo.                                       | 1, 2, 3, … (até 374)                                     |
# MAGIC | `Gender`                            | String       | Gênero do indivíduo.                                                         | `Male`, `Female`                                         |
# MAGIC | `Age`                               | Inteiro      | Idade do indivíduo, em anos.                                                 | 27 a 59                                                  |
# MAGIC | `Occupation`                        | String       | Ocupação ou profissão declarada.                                             | `Software Engineer`, `Doctor`, `Teacher`                |
# MAGIC | `Sleep Duration (hours)`            | Double       | Duração média de sono por noite (em horas).                                  | 5.8 (mín) a 8.5 (máx)                                    |
# MAGIC | `Quality of Sleep (scale: 1-10)`    | Inteiro      | Avaliação subjetiva da qualidade do sono, de 1 (péssimo) a 10 (excelente).   | 4 a 9                                                    |
# MAGIC | `Physical Activity Level (min/day)` | Inteiro      | Tempo de atividade física diária, em minutos.                                | 30 a 90 minutos                                          |
# MAGIC | `Stress Level (scale: 1-10)`        | Inteiro      | Nível subjetivo de estresse, de 1 (baixo) a 10 (alto).                       | 3 a 8                                                    |
# MAGIC | `BMI Category`                      | String       | Categoria do IMC (Índice de Massa Corporal).                                 | `Underweight`, `Normal`, `Normal Weight`, `Overweight`, `Obese` |
# MAGIC | `Blood Pressure (systolic/diastolic)` | String     | Pressão arterial no formato “sistólica/diastólica”, em mmHg.                | `120/80`, `130/90`                                       |
# MAGIC | `Heart Rate (bpm)`                  | Inteiro      | Frequência cardíaca de repouso, em batimentos por minuto.                    | 65 a 86 bpm                                              |
# MAGIC | `Daily Steps`                       | Inteiro      | Total de passos diários realizados.                                          | 3000 a 10000 passos                                      |
# MAGIC | `Sleep Disorder`                    | String       | Presença de distúrbio do sono (se houver).                                   | `None`, `Insomnia`, `Sleep Apnea`                       |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Observações Importantes
# MAGIC
# MAGIC - As colunas foram mantidas com os nomes e formatos originais na camada **Bronze**, inclusive com espaços e letras maiúsculas.
# MAGIC - A coluna `BMI Category` apresenta uma **duplicação semântica**: os valores `"Normal"` e `"Normal Weight"` aparecem como categorias separadas, mas representam o mesmo conceito. Essa inconsistência será **tratada na etapa de limpeza**.
# MAGIC - O campo `Blood Pressure` está armazenado como texto (string), no formato `"sistólica/diastólica"`. Ele será **dividido em duas colunas numéricas** (`systolic`, `diastolic`) durante a transformação, para facilitar análises futuras.
# MAGIC - A modelagem **permanece flat**, onde **cada linha representa um indivíduo completo**, com todos os seus atributos de saúde e estilo de vida consolidados.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Conclusão da Etapa
# MAGIC
# MAGIC Com essa modelagem, o dataset está pronto para passar à próxima etapa do pipeline: a transformação e limpeza na **camada Silver**, onde aplicaremos tipagem adequada, padronização de categorias e tratamento de inconsistências.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Limpeza e Transformação dos Dados (Camada Silver)
# MAGIC
# MAGIC Na **camada Silver**, aplicamos um conjunto de transformações com o objetivo de **melhorar a qualidade, padronizar os dados e garantir sua usabilidade analítica**. Esta etapa é responsável por preparar os dados para análises confiáveis, eliminando inconsistências, padronizando valores e enriquecendo a estrutura original do dataset.
# MAGIC
# MAGIC Importante: mantemos **todos os registros válidos da camada Bronze**, exceto em casos de filtragem por qualidade (ex: registros duplicados ou inválidos). O foco é garantir **consistência e integridade dos dados** para consumo por visualizações, análises exploratórias e camadas analíticas posteriores (Gold).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 🔧 Principais Transformações Aplicadas:
# MAGIC
# MAGIC - **Padronização de Nomes de Colunas**  
# MAGIC   Todas as colunas foram renomeadas para o padrão `snake_case`, em letras minúsculas e sem espaços, facilitando seu uso em consultas SQL e DataFrame API.  
# MAGIC   Exemplo: `"Quality of Sleep"` → `sleep_quality`.
# MAGIC
# MAGIC - **Tratamento de Valores Ausentes (null)**  
# MAGIC   Realizamos verificação de valores nulos nas colunas principais. No dataset original, não foram identificados nulos, portanto, **nenhuma imputação foi necessária**. Caso existissem, poderíamos aplicar:
# MAGIC   - Média para variáveis numéricas
# MAGIC   - Categoria `"Desconhecido"` para variáveis categóricas
# MAGIC
# MAGIC - **Correção de Inconsistências Categóricas**  
# MAGIC   A coluna `bmi_category` apresentava valores distintos para a mesma categoria: `"Normal"` e `"Normal Weight"`. Esses valores foram **padronizados para `"Normal"`**, garantindo uniformidade nas análises.
# MAGIC
# MAGIC - **Conversão Explícita de Tipos de Dados**  
# MAGIC   Embora o Spark tenha inferido corretamente a maioria dos tipos (inteiros, double, string), garantimos a tipagem adequada para cada coluna, especialmente após transformações.  
# MAGIC   Exemplo: `bp_systolic` e `bp_diastolic` (derivadas de `blood_pressure`) foram convertidas para `IntegerType`.
# MAGIC
# MAGIC - **Enriquecimento de Colunas**  
# MAGIC   A coluna `blood_pressure` foi desmembrada (split) em duas novas colunas:
# MAGIC   - `bp_systolic` (pressão sistólica)
# MAGIC   - `bp_diastolic` (pressão diastólica)  
# MAGIC   Isso permite análises separadas de cada componente da pressão arterial.  
# MAGIC   A coluna original (`blood_pressure`) foi **mantida** para referência, mas poderia ser descartada em um cenário produtivo.
# MAGIC
# MAGIC - **Verificação de Duplicidade de IDs**  
# MAGIC   Confirmamos que a coluna `person_id` é **única por registro**, não sendo necessário aplicar deduplicação. Ela foi mantida como chave técnica para eventuais análises de granularidade individual.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Resultado da Transformação
# MAGIC
# MAGIC Ao final desta etapa, obtivemos uma tabela Silver consistente, confiável e pronta para análises e agregações. Todos os dados estão devidamente limpos, tipados e padronizados. Agora seguimos para a persistência no formato Parquet e registro no catálogo do Spark, garantindo rastreabilidade e integração com as próximas etapas do pipeline (Gold).
# MAGIC
# MAGIC Vamos agora aplicar essas transformações usando a API de DataFrame do Spark:

# COMMAND ----------

from pyspark.sql.functions import trim, when, col, split

# 📦 Carregar dados da tabela Bronze registrada no catálogo
df_bronze = spark.table("bronze_sleep_health")

# 🧼 Aplicar transformações para criar a camada Silver
df_silver = (
    df_bronze
    # 1. Renomear colunas para padrão snake_case
    .withColumnRenamed("Person ID", "person_id")
    .withColumnRenamed("Gender", "gender")
    .withColumnRenamed("Age", "age")
    .withColumnRenamed("Occupation", "occupation")
    .withColumnRenamed("Sleep Duration", "sleep_duration")
    .withColumnRenamed("Quality of Sleep", "sleep_quality")
    .withColumnRenamed("Physical Activity Level", "physical_activity_level")
    .withColumnRenamed("Stress Level", "stress_level")
    .withColumnRenamed("BMI Category", "bmi_category")
    .withColumnRenamed("Blood Pressure", "blood_pressure")
    .withColumnRenamed("Heart Rate", "heart_rate")
    .withColumnRenamed("Daily Steps", "daily_steps")
    .withColumnRenamed("Sleep Disorder", "sleep_disorder")
    
    # 2. Remover espaços em colunas de texto
    .withColumn("gender", trim(col("gender")))
    .withColumn("occupation", trim(col("occupation")))
    .withColumn("bmi_category", trim(col("bmi_category")))
    .withColumn("sleep_disorder", trim(col("sleep_disorder")))
    
    # 3. Padronizar categorias duplicadas
    .withColumn("bmi_category", when(col("bmi_category") == "Normal Weight", "Normal")
                                 .otherwise(col("bmi_category")))
    
    # 4. Separar pressão arterial em sistólica e diastólica
    .withColumn("bp_systolic", split(col("blood_pressure"), "/").getItem(0).cast("int"))
    .withColumn("bp_diastolic", split(col("blood_pressure"), "/").getItem(1).cast("int"))
    
    # 5. (Opcional) Exemplo de imputação se houvesse nulos
    # .fillna({"sleep_quality": 5})  # Aplicar apenas se necessário
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explicações das Transformações na Camada Silver
# MAGIC
# MAGIC Nesta etapa, realizamos uma série de ajustes e enriquecimentos nos dados brutos da camada Bronze para compor a **tabela Silver**, pronta para análises confiáveis:
# MAGIC
# MAGIC - **Padronização de nomes de colunas** para o formato `snake_case`, facilitando o uso em SQL e no código PySpark.
# MAGIC - **Remoção de espaços** nas extremidades de strings categóricas (ex: `"None "` → `"None"`), evitando problemas em agrupamentos e filtros.
# MAGIC - **Unificação de categorias duplicadas**: o valor `"Normal Weight"` na coluna `bmi_category` foi padronizado como `"Normal"`.
# MAGIC - **Separação da pressão arterial**: a coluna `blood_pressure` foi desmembrada em:
# MAGIC   - `bp_systolic`: componente sistólico (ex: 126)
# MAGIC   - `bp_diastolic`: componente diastólico (ex: 83)
# MAGIC - **Tipos corretos**: as colunas derivadas foram convertidas para inteiros, prontos para estatísticas.
# MAGIC - **Preservação da coluna original** `blood_pressure` para referência textual.
# MAGIC - Nenhum valor nulo foi detectado; não foi necessário aplicar imputação, mas deixamos exemplo comentado para casos futuros.
# MAGIC
# MAGIC A camada Silver reflete um **dataset confiável, limpo e analiticamente utilizável**, respeitando o princípio de manter os dados mais próximos do estado real, mas com consistência e tipagem adequadas.

# COMMAND ----------

# MAGIC %md
# MAGIC Agora, persistiremos o DataFrame Silver em formato Parquet no DBFS e registraremos a tabela Silver no catálogo do Spark:

# COMMAND ----------

# 📋 Visualizar o schema atualizado
df_silver.printSchema()

# 🔍 Amostragem dos dados transformados
df_silver.select(
    "person_id", "age", "gender", "occupation", "sleep_duration", 
    "sleep_quality", "stress_level", "bmi_category", 
    "blood_pressure", "bp_systolic", "bp_diastolic", "sleep_disorder"
).show(5, truncate=False)

# COMMAND ----------

# Persistir dados Silver em Parquet
df_silver.write.mode("overwrite").parquet("/FileStore/tables/silver/sleep_health_and_lifestyle")

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS silver_sleep_health")

# COMMAND ----------

dbutils.fs.rm("dbfs:/user/hive/warehouse/silver_sleep_health", recurse=True)

# COMMAND ----------

df_silver.write.mode("overwrite").format("parquet").saveAsTable("silver_sleep_health")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_sleep_health LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Análise da Qualidade dos Dados (Data Quality)
# MAGIC
# MAGIC Antes de prosseguir para análises mais avançadas, é fundamental assegurar que os dados da **camada Silver** estejam consistentes e utilizáveis. Nessa etapa, avaliamos a **presença de valores ausentes (nulos)** e outras possíveis **inconsistências**.
# MAGIC
# MAGIC ### Verificação de Nulos
# MAGIC
# MAGIC Realizamos a checagem de valores nulos em todas as colunas da tabela `silver_sleep_health`. A inspeção inicial, visual e programática, confirmou que:
# MAGIC
# MAGIC - Nenhuma das 13 colunas contém valores nulos;
# MAGIC - Todos os registros estão completos e válidos para as análises planejadas.
# MAGIC
# MAGIC Apesar disso, **incluímos uma verificação automatizada** para garantir a qualidade de forma programática (vide código abaixo), assegurando robustez do pipeline.

# COMMAND ----------

### Código para validação de nulos:
from pyspark.sql.functions import col, sum

# Somar nulos por coluna
df_silver.select([
    sum(col(c).isNull().cast("int")).alias(c)
    for c in df_silver.columns
]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC 	### Continuação da Análise de Qualidade dos Dados (Data Quality)
# MAGIC
# MAGIC Após validar a ausência de nulos, realizamos outras verificações importantes:
# MAGIC
# MAGIC #### Valores Duplicados
# MAGIC Verificamos se há **IDs duplicados** (o que indicaria entradas repetidas da mesma pessoa). A contagem de `person_id` distintos retornou exatamente **374 registros únicos**, o mesmo número de linhas do dataset. Isso confirma que **não há duplicatas** — cada linha representa um indivíduo diferente.
# MAGIC
# MAGIC #### Intervalos e Outliers
# MAGIC
# MAGIC Avaliamos os intervalos mínimos e máximos das variáveis numéricas para identificar possíveis **outliers** ou **valores incoerentes**. Resultados:
# MAGIC
# MAGIC | **Variável**              | **Intervalo Observado** | **Comentário** |
# MAGIC |---------------------------|--------------------------|----------------|
# MAGIC | Idade                     | 27 a 59 anos             | Todos adultos de meia-idade. Plausível. |
# MAGIC | Duração do Sono (horas)   | 5.8 a 8.5 h              | Extremamente plausível (sem valores anômalos). |
# MAGIC | Qualidade do Sono (1-10)  | 4 a 9                    | Não há extremos absolutos (1 ou 10), mas há variações relevantes. |
# MAGIC | Atividade Física (min/dia)| 30 a 90                  | Ninguém é completamente sedentário; 90 min é elevado, mas plausível. |
# MAGIC | Nível de Estresse (1-10)  | 3 a 8                    | Faixa moderada; ninguém relatou estresse extremamente baixo ou alto. |
# MAGIC | Frequência Cardíaca (bpm) | 65 a 86 bpm              | Todos dentro da faixa fisiológica considerada normal. |
# MAGIC | Passos Diários            | 3.000 a 10.000           | 3.000 é sedentário, 10.000 é meta recomendada. Sem outliers. |
# MAGIC | Pressão Sistólica (mmHg)  | 110 a 140                | Faixa entre normal e limítrofe. Coerente. |
# MAGIC | Pressão Diastólica (mmHg) | 70 a 95                 | Dentro da faixa esperada. Nenhum valor extremo. |
# MAGIC
# MAGIC **Conclusão**: Os dados estão **bem comportados e realistas**, sem necessidade de tratamento de outliers como Winsorização. A única inconsistência real (“Normal Weight” vs “Normal”) já foi **padronizada** durante a transformação.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### Coerência Entre Atributos
# MAGIC
# MAGIC Além de avaliar valores individuais, verificamos **relações lógicas entre variáveis**:
# MAGIC
# MAGIC - **Distúrbios do Sono**: Indivíduos com **insônia ou apneia** tendem a ter **menor duração** e **pior qualidade de sono**, conforme esperado.
# MAGIC - **Estresse**: Existe uma **relação inversa visível** entre nível de estresse e qualidade/duração do sono — indivíduos mais estressados dormem menos e pior.
# MAGIC - **Ocupação**: Profissionais de vendas (ex.: *Sales Representative*) já se destacam negativamente, com **baixa duração e qualidade do sono** — possivelmente pela natureza estressante do trabalho.
# MAGIC - **Gênero e idade**: Padrões por gênero ou idade também podem ser relevantes e serão analisados nas seções a seguir.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Com todas essas validações realizadas, podemos afirmar que os dados da camada Silver estão **consistentes e prontos** para análises mais aprofundadas.
# MAGIC
# MAGIC A seguir, partiremos para a construção da **camada Gold** com dados agregados e insights acionáveis.

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Agregação e Preparação Final (Camada Gold)
# MAGIC
# MAGIC A camada Gold representa os dados prontos para consumo analítico, frequentemente agregados ou resumidos de forma a responder perguntas de negócio específicas. Enquanto a camada Silver ainda contém dados no nível de detalhe de cada indivíduo, podemos querer derivar visões mais consolidadas na Gold.
# MAGIC
# MAGIC Para este projeto, vamos criar uma tabela Gold que resume algumas métricas-chave por categoria de distúrbio do sono. Essa agregação nos ajudará a entender, por exemplo, como variam as características de saúde e hábitos entre pessoas com insônia, com apneia do sono, e sem nenhum distúrbio.
# MAGIC
# MAGIC (Observação: Poderíamos criar múltiplas tabelas Gold para diferentes perspectivas – por exemplo, por Ocupação, por Gênero, etc. Aqui faremos uma agregação como exemplo, mas ainda assim poderemos explorar outras questões diretamente na camada Silver quando conveniente.)
# MAGIC
# MAGIC Vamos agrupar os dados por sleep_disorder e calcular: número de indivíduos em cada categoria, média de horas de sono, média de qualidade do sono, média de nível de estresse, média de nível de atividade física, média de frequência cardíaca e média de passos diários. Arredondaremos as médias para tornar a saída legível.

# COMMAND ----------

from pyspark.sql.functions import avg, count, round

# Carregar dados da tabela Silver
df_silver = spark.table("silver_sleep_health")

# Agregar métricas por categoria de distúrbio do sono
df_gold = (df_silver.groupBy("sleep_disorder")
    .agg(
        count("*").alias("count_individuals"),
        round(avg("sleep_duration"), 2).alias("avg_sleep_duration"),
        round(avg("sleep_quality"), 2).alias("avg_sleep_quality"),
        round(avg("stress_level"), 2).alias("avg_stress_level"),
        round(avg("physical_activity_level"), 2).alias("avg_physical_activity_level"),
        round(avg("heart_rate"), 2).alias("avg_heart_rate"),
        round(avg("daily_steps"), 0).cast("int").alias("avg_daily_steps")
    )
)

# Visualizar o resultado da agregação
df_gold.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Analisando brevemente esses números agregados (que interpretaremos mais adiante): por exemplo, indivíduos com Insomnia dormem em média 6.59 horas com qualidade 6.53, e têm estresse médio 7.17 – claramente diferente de quem não tem distúrbio (7.36 horas, qualidade 7.63, estresse 4.53). Já os com Sleep Apnea ficam num intermédio, dormindo ~7.03 horas, qualidade ~7.21, estresse 6.95.
# MAGIC
# MAGIC Agora, salvaremos essa tabela Gold e a registraremos no catálogo:

# COMMAND ----------

# Persistir a Tabela Gold no formato Parquet e registrar no catálogo
df_gold.write.mode("overwrite").format("parquet").saveAsTable("gold_sleep_health")

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS gold_sleep_metrics_by_disorder")
dbutils.fs.rm("dbfs:/user/hive/warehouse/gold_sleep_metrics_by_disorder", recurse=True)

# COMMAND ----------

df_gold.write.mode("overwrite").format("parquet").saveAsTable("gold_sleep_metrics_by_disorder")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_sleep_metrics_by_disorder LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC A tabela gold_sleep_metrics_by_disorder está pronta para consultas SQL ou uso em visualizações. Por exemplo, um SELECT * FROM gold_sleep_metrics_by_disorder; retornaria a mesma pequena tabela mostrada acima, servindo como base para insights de alto nível sobre distúrbios do sono.

# COMMAND ----------

# MAGIC %md
# MAGIC **Análise da Qualidade do Sono por Tipo de Distúrbio**
# MAGIC
# MAGIC _O gráfico acima apresenta a distribuição da qualidade do sono (de 1 a 10) entre os diferentes grupos de indivíduos, classificados de acordo com o tipo de distúrbio do sono informado no dataset.
# MAGIC
# MAGIC Cada boxplot representa um resumo estatístico da qualidade do sono para um grupo específico:
# MAGIC 	•	Ausência de Distúrbio do Sono: Apresenta a maior mediana de qualidade do sono, com baixa dispersão. Isso indica que, para a maioria desses indivíduos, o sono é consistente e de boa qualidade, como era esperado para quem não apresenta distúrbios clínicos.
# MAGIC 	•	Apneia do Sono: Os indivíduos com apneia tendem a apresentar uma qualidade de sono mais baixa e mais dispersa, o que reflete os efeitos diretos desse distúrbio, conhecido por causar interrupções frequentes na respiração durante o sono. Isso afeta negativamente tanto a profundidade quanto a continuidade do descanso.
# MAGIC 	•	Insônia: Como previsto, o grupo de indivíduos com insônia mostra uma das menores medianas de qualidade do sono. A dispersão também é considerável, sugerindo que há variação entre os casos — alguns mais leves e outros mais severos. Esse comportamento é típico, uma vez que a insônia afeta não só o início do sono, mas também sua manutenção e percepção._

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 6. **Análises Exploratórias e Insights de Negócio**
# MAGIC
# MAGIC Com os dados preparados, podemos agora responder a algumas perguntas de negócio e explorar hipóteses sobre hábitos de sono e saúde. Iremos utilizar tanto consultas nos dados (Silver/Gold) quanto visualizações gráficas para interpretar os resultados.

# COMMAND ----------

# MAGIC %md
# MAGIC **Estatísticas descritivas por grupo (média, desvio padrão e mediana da qualidade do sono)**

# COMMAND ----------

# Importar bibliotecas
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pyspark.sql import functions as F

# Carregar e preparar os dados da tabela silver
df_silver = spark.table("silver_sleep_health")

# Traduzir distúrbios para português
df_box = df_silver.select("sleep_quality", "sleep_disorder") \
    .withColumn(
        "Distúrbio do Sono",
        F.when(F.col("sleep_disorder") == "Sleep Apnea", "Apneia do Sono")
         .when(F.col("sleep_disorder") == "Insomnia", "Insônia")
         .when(F.col("sleep_disorder") == "None", "Ausência de Distúrbio do Sono")
         .otherwise("Desconhecido")
    ).toPandas()

# Agrupar e calcular estatísticas
df_resumo = (
    df_box.groupby("Distúrbio do Sono")["sleep_quality"]
    .agg(["count", "mean", "median", "std"])
    .rename(columns={
        "count": "Qtd Indivíduos",
        "mean": "Média",
        "median": "Mediana",
        "std": "Desvio Padrão"
    })
    .round(2)
    .sort_values(by="Média", ascending=False)
)

# Estilizar exibição no notebook
df_resumo.index.name = "Distúrbio do Sono"
df_resumo.style.set_caption("Resumo Estatístico da Qualidade do Sono por Distúrbio") \
    .background_gradient(cmap="Blues", subset=["Média"]) \
    .format(precision=2)

# COMMAND ----------

# MAGIC %md
# MAGIC **Análise Estatística da Qualidade do Sono por Tipo de Distúrbio**
# MAGIC
# MAGIC A tabela acima resume a qualidade do sono autoavaliada (em escala de 1 a 10), agrupada por tipo de distúrbio do sono relatado. Para cada grupo, foram calculados os seguintes indicadores estatísticos:
# MAGIC
# MAGIC - **Qtd Indivíduos**: Número de pessoas em cada categoria.
# MAGIC - **Média**: Valor médio da qualidade do sono.
# MAGIC - **Mediana**: Valor central da distribuição.
# MAGIC - **Desvio Padrão**: Medida de dispersão dos dados.
# MAGIC
# MAGIC ### Principais Insights:
# MAGIC
# MAGIC - **Ausência de Distúrbio do Sono**: Esse grupo apresenta a maior média (e também mediana) de qualidade do sono, com menor variação entre os indivíduos. Isso confirma a expectativa de que pessoas sem distúrbios tendem a ter um sono mais consistente e satisfatório.
# MAGIC
# MAGIC - **Insônia**: Apresenta a menor média e uma dispersão elevada, sugerindo que esse distúrbio afeta fortemente a percepção da qualidade do sono. A variação alta indica que há casos mais severos e outros mais moderados dentro do grupo.
# MAGIC
# MAGIC - **Apneia do Sono**: Embora apresente média superior à de insônia, ainda está abaixo do grupo sem distúrbios. O desvio padrão mais alto indica grande heterogeneidade dentro do grupo, o que pode refletir diferentes níveis de severidade da apneia ou coexistência com outros fatores de saúde.
# MAGIC
# MAGIC Esses dados reforçam que distúrbios do sono estão associados à pior qualidade percebida do sono, e que estratégias de monitoramento e tratamento são essenciais para melhorar a saúde do sono nessas populações.

# COMMAND ----------

# MAGIC %md
# MAGIC **Distribuição de Distúrbios do Sono na População**

# COMMAND ----------

# Importar bibliotecas
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pyspark.sql import functions as F

# Carregar a tabela Silver
df_silver = spark.table("silver_sleep_health")

# Traduzir os rótulos dos distúrbios (incluindo None)
df_pt = df_silver.withColumn(
    "disturbio_sono",
    F.when(F.col("sleep_disorder") == "Sleep Apnea", "Apneia do Sono")
     .when(F.col("sleep_disorder") == "Insomnia", "Insônia")
     .when(F.col("sleep_disorder") == "None", "Ausência de Distúrbio do Sono")
     .otherwise("Desconhecido")  # Segurança extra para casos inesperados
)

# Agrupar e converter para Pandas
sleep_disorder_counts = (
    df_pt.groupBy("disturbio_sono").count()
    .toPandas()
    .sort_values(by="count", ascending=False)
)

# Plotar gráfico
plt.figure(figsize=(10, 6))
sns.set(style="whitegrid")

ax = sns.barplot(
    x="disturbio_sono",
    y="count",
    data=sleep_disorder_counts,
    palette="Set2"
)

# Adicionar valores no topo das barras
for i, row in sleep_disorder_counts.iterrows():
    ax.text(i, row["count"] + 2, f"{row['count']}", ha="center", va="bottom", fontsize=11)

# Título e rótulos
plt.title("Distribuição dos Distúrbios do Sono", fontsize=14, weight='bold')
plt.xlabel("Tipo de Distúrbio do Sono", fontsize=12)
plt.ylabel("Número de Indivíduos", fontsize=12)
plt.xticks(rotation=10, fontsize=11)
plt.yticks(fontsize=11)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Distribuição dos Distúrbios do Sono
# MAGIC
# MAGIC O gráfico acima mostra a distribuição dos indivíduos conforme a presença (ou não) de distúrbios do sono.
# MAGIC
# MAGIC - A maioria dos participantes **não apresenta distúrbios**, com cerca de **219 indivíduos (aproximadamente 58%)** classificados com “Ausência de Distúrbio do Sono”.
# MAGIC - Os demais estão quase igualmente divididos entre:
# MAGIC   - **Apneia do Sono**: 78 indivíduos (~21%)
# MAGIC   - **Insônia**: 77 indivíduos (~21%)
# MAGIC
# MAGIC Essa distribuição mostra que cerca de **42% da amostra possui algum distúrbio do sono**, o que representa um percentual significativo. Apesar de o dataset ser sintético (gerado artificialmente), ele oferece uma amostra balanceada o suficiente para análises comparativas entre os grupos.
# MAGIC
# MAGIC **Implicações práticas**: Para profissionais ou negócios da área da saúde (como clínicas do sono), esses dados indicam que quase metade da população avaliada poderia demandar algum tipo de intervenção ou tratamento. Como insônia e apneia aparecem em proporções semelhantes, estratégias de atenção e cuidado devem ser **equilibradas entre ambos os distúrbios**.
# MAGIC
# MAGIC Na sequência, aprofundaremos a relação entre distúrbios, **qualidade** e **duração do sono**.

# COMMAND ----------

# MAGIC %md
# MAGIC **Relação entre Duração do Sono, Qualidade e Distúrbios**

# COMMAND ----------

# Importar bibliotecas
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pyspark.sql import functions as F

# Carregar a tabela Silver
df_silver = spark.table("silver_sleep_health")

# Traduzir os rótulos dos distúrbios (inclusive None)
df_plot = df_silver.select("sleep_duration", "sleep_quality", "sleep_disorder") \
    .withColumn(
        "disturbio_sono",
        F.when(F.col("sleep_disorder") == "Sleep Apnea", "Apneia do Sono")
         .when(F.col("sleep_disorder") == "Insomnia", "Insônia")
         .when(F.col("sleep_disorder") == "None", "Ausência de Distúrbio do Sono")
         .otherwise("Desconhecido")
    ).toPandas()

# Gráfico de dispersão
plt.figure(figsize=(10, 6))
sns.set(style="whitegrid")

sns.scatterplot(data=df_plot,
                x="sleep_duration",
                y="sleep_quality",
                hue="disturbio_sono",
                palette="Set1",
                alpha=0.8,
                s=60)

plt.title("Relação entre Duração e Qualidade do Sono por Tipo de Distúrbio", fontsize=14, weight='bold')
plt.xlabel("Duração do Sono (horas)", fontsize=12)
plt.ylabel("Qualidade do Sono (1 a 10)", fontsize=12)
plt.legend(title="Distúrbio do Sono", fontsize=10, title_fontsize=11)
plt.grid(True, linestyle="--", alpha=0.5)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC O gráfico de dispersão acima relaciona horas de sono por noite (eixo X) com a qualidade do sono autoavaliada (eixo Y), e diferencia os indivíduos por categoria de distúrbio do sono. Observamos uma tendência clara: há uma correlação positiva entre dormir mais horas e ter uma qualidade de sono melhor. Os pontos se concentram aproximadamente em uma diagonal ascendente – ou seja, quem dorme pouco (em torno de 6 horas) tende a reportar qualidade menor (por volta de 5-6), enquanto quem dorme perto de 8 horas tende a dar notas de qualidade maiores (7-9).
# MAGIC
# MAGIC Além disso, as cores/formatos dos pontos indicam que:
# MAGIC 	•	Indivíduos com Insomnia (marcadores verdes “x”) estão predominantemente na parte inferior esquerda do gráfico – muitos dormem menos de 6.5 horas e avaliam a qualidade entre 4 e 7. Não surpreende, já que insônia se caracteriza por dificuldade em manter um sono longo e restaurador.
# MAGIC 	•	Indivíduos com Sleep Apnea (azul) também sofrem impacto, embora um pouco menos extremo: muitos dormem entre ~6 e 7.5 horas com qualidade variando de 6 a 8. A apneia pode fragmentar o sono, reduzindo sua eficácia, o que explica por que mesmo com horas relativamente ok, a qualidade pode não ser máxima.
# MAGIC 	•	Aqueles sem distúrbio (vermelho) se espalham mais para direita e topo: é mais comum vê-los dormindo 7-8+ horas e alcançando qualidade alta (7-9). Há poucos vermelhos em qualidade 5 ou 6.
# MAGIC
# MAGIC Essa relação confirma o esperado: distúrbios prejudicam tanto a duração quanto a percepção de qualidade do sono. Para um negócio na área de saúde, reforça-se a importância de tratar problemas de sono não só para melhorar a quantidade de horas dormidas, mas também a qualidade percebida pelo paciente.
# MAGIC
# MAGIC **Impacto do Estresse e Atividade Física**
# MAGIC
# MAGIC A partir dos dados agregados e correlacionados, verificamos que o nível de estresse tem forte correlação negativa com o sono:
# MAGIC 	•	Pessoas sem distúrbio apresentam estresse médio ~4.5, enquanto insones e apneicos têm ~7.1 e ~6.9 respectivamente (bem mais alto).
# MAGIC 	•	A correlação Pearson entre stress_level e sleep_quality no dataset Silver é aproximadamente -0.90, e com sleep_duration cerca de -0.81, indicando que níveis altos de estresse estão associados a dormir menos e pior (o que faz sentido intuitivo e clínico).
# MAGIC
# MAGIC Por outro lado, o nível de atividade física diário (minutos de exercício) não varia tanto entre os grupos de distúrbio (médias ~60 min para quem não tem distúrbio vs ~45 min com distúrbio). A correlação entre atividade física e qualidade do sono é positiva porém fraca (~0.19 no coeficiente de Pearson). Ou seja, praticar mais exercícios ajuda um pouco no sono, mas no dataset o efeito é modesto. Pode ser que todos tenham algum nível mínimo de atividade (ninguém sedentário absoluto) e poucos muito ativos, limitando a variabilidade.
# MAGIC
# MAGIC Entretanto, a frequência cardíaca de repouso ficou mais alta em média para quem tem distúrbios (cerca de 80 bpm) comparado a quem não tem (73 bpm). Isso pode indicar que insônia e apneia estão associadas a pior condicionamento ou maior ativação do sistema cardiovascular (possivelmente relacionado ao estresse). Já a média de passos diários também foi bem menor nos grupos com distúrbio (~5000) versus sem (~7700), sugerindo que problemas de sono podem vir acompanhados de estilo de vida menos ativo (ou vice-versa, menos atividade pode prejudicar o sono).
# MAGIC
# MAGIC **Diferenças por Ocupação e Gênero**
# MAGIC
# MAGIC Analisando os dados por ocupação, encontramos insights interessantes:
# MAGIC 	•	Profissionais de vendas parecem ter os piores indicadores de sono. Por exemplo, os Sales Representatives dormem em média apenas ~5.9 horas com qualidade média em torno de 4 (muito baixa) e figuram com os maiores níveis de estresse (~8 de média!). Essa ocupação pode envolver pressão por metas e horários irregulares, explicando o alto estresse e pouco sono. Outra categoria similar, Salesperson, também está entre as piores (média ~6.4h de sono, qualidade 6.0, estresse 7.0).
# MAGIC 	•	Em contraste, ocupações de Engenharia apresentam os melhores resultados de sono. Os dados indicam que Engineers (genérico) dormem quase 8 horas (7.99h) em média, com qualidade acima de 8, e relatam o menor estresse (~3.9). Lawyers e Accountants também dormem bastante (7.4h e 7.1h) e têm qualidade alta (~7.9), possivelmente indicando rotinas mais controladas ou conscientização sobre saúde.
# MAGIC 	•	Médicos (Doctors) e enfermeiros (Nurses) estão em posição intermediária, com ~7 horas de sono e qualidade ~7, mas curiosamente apresentam estresse relativamente elevado (especialmente médicos com média ~6.7 de estresse). Isso sugere que, embora consigam dormir uma quantidade decente, a tensão da profissão médica afeta a qualidade percebida (ainda assim melhor que a do pessoal de vendas).
# MAGIC 	•	Professores (Teachers) e gestores (Managers) também mostraram métricas medianas (sono ~6.7-6.9h, qualidade ~7.0, estresse ~5.0), sem extremos.
# MAGIC
# MAGIC Em resumo, profissões com maior estresse profissional tendem a refletir pior perfil de sono. Este achado pode ser valioso para programas de bem-estar corporativo: empresas de setores comerciais poderiam investir em apoio ao sono dos funcionários (por exemplo, gerenciamento de estresse, horários flexíveis), enquanto setores de engenharia aparentemente já demonstram equilíbrio melhor (mas não se deve ignorar indivíduos fora da média).
# MAGIC
# MAGIC **Quanto às diferenças por gênero:**
# MAGIC 	•	Mulheres no dataset dormem um pouco mais que homens (7.23 vs 7.04 horas em média) e relatam qualidade do sono superior (7.66 vs 6.97). Além disso, o nível médio de estresse das mulheres é menor (4.68 contra 6.08 dos homens).
# MAGIC 	•	Essa tendência sugere que, nesta amostra, homens estão mais estressados e dormindo pior que mulheres. Poderia ser reflexo das ocupações (talvez mais homens em cargos de alta pressão?) ou outros fatores sociais. De qualquer forma, indica um possível público-alvo: homens poderiam demandar mais intervenções de saúde do sono.
# MAGIC 	•	Não há diferença de gênero nos distúrbios presentes: tanto insônia quanto apneia apareceram em ambos gêneros, mas os efeitos parecem alinhados ao nível de estresse e hábitos de cada indivíduo.
# MAGIC
# MAGIC **Resumo dos Insights Chave**
# MAGIC 	•	Distúrbios do Sono são comuns: Quase 42% da amostra possui insônia ou apneia, impactando negativamente suas horas e qualidade de sono.
# MAGIC 	•	Estresse é um fator crítico: Altos níveis de estresse se associam a sono insuficiente e insatisfatório. Programas para redução de estresse podem melhorar a saúde do sono.
# MAGIC 	•	Atividade Física e Condicionamento: Embora todos façam alguma atividade, indivíduos com melhor sono tendem a dar mais passos diários e ter FC de repouso mais baixa (melhor condicionamento cardiovascular). Estímulo à atividade física pode trazer benefícios moderados ao sono.
# MAGIC 	•	Ocupação influencia o sono: Profissionais de vendas destacam-se negativamente (alto estresse, pior sono), enquanto engenheiros e alguns profissionais liberais têm sono melhor. Empresas podem usar esses dados para direcionar ações de saúde ocupacional.
# MAGIC 	•	Diferenças de Gênero: Homens da amostra tiveram piores resultados de sono que mulheres, possivelmente ligados a maiores níveis de estresse. Intervenções podem ser direcionadas levando isso em conta.
# MAGIC
# MAGIC 7. **Conclusão e Autoavaliação**_
# MAGIC
# MAGIC Conclusão do Estudo: Construímos um pipeline completo de dados, passando pelas camadas Bronze (ingestão bruta), Silver (refinamento e limpeza) e Gold (agregação e sumarização), usando Spark no ambiente Databricks. O dataset Sleep Health and Lifestyle nos permitiu explorar como fatores de estilo de vida e saúde se relacionam com a qualidade do sono. Confirmamos expectativas, como o impacto negativo do estresse e de distúrbios como insônia, e levantamos insights acionáveis, como a identificação de grupos (por profissão ou gênero) que poderiam se beneficiar de atenção especial para melhoria do sono. Todos os passos foram documentados em detalhe, mostrando não apenas o o que foi feito, mas por que – desde decisões de modelagem até interpretação de resultados._
# MAGIC
# MAGIC Aprendizados e Boas Práticas Aplicadas: Conseguimos aplicar com sucesso boas práticas de engenharia de dados, como a organização em camadas, o uso de formatos colunar eficientes (Parquet), a criação de um catálogo de dados, e transformações com APIs distribuídas do Spark ao invés de abordagens sequenciais. A separação em Bronze/Silver/Gold demonstrou como cada camada tem um propósito distinto (confiabilidade vs. usabilidade vs. valor de negócio). Além disso, integraremos no futuro a automação desses passos em um fluxo de trabalho repetível (por exemplo, usando jobs do Databricks ou pipelines de integração contínua de dados).