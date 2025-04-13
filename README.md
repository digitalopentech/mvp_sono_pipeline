Análise da Qualidade do Sono com Engenharia de Dados no Databricks

Este projeto tem como objetivo analisar como fatores de estilo de vida e saúde influenciam a qualidade do sono, utilizando técnicas de engenharia de dados e análise exploratória com Spark e Pandas em ambiente Databricks.

Objetivo

Investigar, com base em dados sintéticos do Kaggle, a relação entre distúrbios do sono e variáveis como:
	•	Estresse
	•	Atividade física
	•	Frequência cardíaca
	•	Ocupação
	•	Gênero

Perguntas de Negócio
	1.	Quais fatores influenciam a qualidade do sono?
	2.	O nível de estresse impacta a duração e qualidade do sono?
	3.	Existem ocupações associadas a pior ou melhor qualidade de sono?
	4.	Atividade física e IMC se relacionam com a qualidade do sono?

Dataset
	•	Fonte: Kaggle - Sleep Health and Lifestyle Dataset
	•	Registros: 374 indivíduos
	•	Colunas: 13 atributos como idade, ocupação, qualidade do sono, distúrbios, pressão arterial, frequência cardíaca, etc.
	•	Tipo: Sintético (ideal para fins educacionais)

Arquitetura do Pipeline de Dados

Foi implementado um pipeline completo com três camadas de dados:

Bronze (Raw)
	•	Dados brutos lidos a partir de CSV com inferência de schema automática.
	•	Nenhuma transformação ou filtragem.

Silver (Refinado)
	•	Padronização de nomes de colunas (snake_case).
	•	Correção de categorias duplicadas (“Normal Weight” → “Normal”).
	•	Separação da coluna blood_pressure em bp_systolic e bp_diastolic.
	•	Verificação de duplicatas, nulos e intervalos válidos para detectar outliers.

Gold (Agregado)
	•	Métricas agregadas por tipo de distúrbio do sono.
	•	Inclui médias de estresse, passos diários, duração do sono, etc.
	•	Dados prontos para consumo analítico e visualizações.

Tecnologias Utilizadas
	•	Apache Spark (PySpark) – Leitura, transformação, agregação
	•	Pandas – Conversão para visualizações com Matplotlib/Seaborn
	•	Databricks Community Edition – Ambiente unificado
	•	Python (Seaborn, Matplotlib) – Gráficos e visualizações
	•	SQL (SparkSQL) – Consultas diretas na camada Gold

