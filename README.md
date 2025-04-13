%md
O gráfico de dispersão acima relaciona horas de sono por noite (eixo X) com a qualidade do sono autoavaliada (eixo Y), e diferencia os indivíduos por categoria de distúrbio do sono. Observamos uma tendência clara: há uma correlação positiva entre dormir mais horas e ter uma qualidade de sono melhor. Os pontos se concentram aproximadamente em uma diagonal ascendente – ou seja, quem dorme pouco (em torno de 6 horas) tende a reportar qualidade menor (por volta de 5-6), enquanto quem dorme perto de 8 horas tende a dar notas de qualidade maiores (7-9).

Além disso, as cores/formatos dos pontos indicam que:
	•	Indivíduos com Insomnia (marcadores verdes “x”) estão predominantemente na parte inferior esquerda do gráfico – muitos dormem menos de 6.5 horas e avaliam a qualidade entre 4 e 7. Não surpreende, já que insônia se caracteriza por dificuldade em manter um sono longo e restaurador.
	•	Indivíduos com Sleep Apnea (azul) também sofrem impacto, embora um pouco menos extremo: muitos dormem entre ~6 e 7.5 horas com qualidade variando de 6 a 8. A apneia pode fragmentar o sono, reduzindo sua eficácia, o que explica por que mesmo com horas relativamente ok, a qualidade pode não ser máxima.
	•	Aqueles sem distúrbio (vermelho) se espalham mais para direita e topo: é mais comum vê-los dormindo 7-8+ horas e alcançando qualidade alta (7-9). Há poucos vermelhos em qualidade 5 ou 6.

Essa relação confirma o esperado: distúrbios prejudicam tanto a duração quanto a percepção de qualidade do sono. Para um negócio na área de saúde, reforça-se a importância de tratar problemas de sono não só para melhorar a quantidade de horas dormidas, mas também a qualidade percebida pelo paciente.

**Impacto do Estresse e Atividade Física**

A partir dos dados agregados e correlacionados, verificamos que o nível de estresse tem forte correlação negativa com o sono:
	•	Pessoas sem distúrbio apresentam estresse médio ~4.5, enquanto insones e apneicos têm ~7.1 e ~6.9 respectivamente (bem mais alto).
	•	A correlação Pearson entre stress_level e sleep_quality no dataset Silver é aproximadamente -0.90, e com sleep_duration cerca de -0.81, indicando que níveis altos de estresse estão associados a dormir menos e pior (o que faz sentido intuitivo e clínico).

Por outro lado, o nível de atividade física diário (minutos de exercício) não varia tanto entre os grupos de distúrbio (médias ~60 min para quem não tem distúrbio vs ~45 min com distúrbio). A correlação entre atividade física e qualidade do sono é positiva porém fraca (~0.19 no coeficiente de Pearson). Ou seja, praticar mais exercícios ajuda um pouco no sono, mas no dataset o efeito é modesto. Pode ser que todos tenham algum nível mínimo de atividade (ninguém sedentário absoluto) e poucos muito ativos, limitando a variabilidade.

Entretanto, a frequência cardíaca de repouso ficou mais alta em média para quem tem distúrbios (cerca de 80 bpm) comparado a quem não tem (73 bpm). Isso pode indicar que insônia e apneia estão associadas a pior condicionamento ou maior ativação do sistema cardiovascular (possivelmente relacionado ao estresse). Já a média de passos diários também foi bem menor nos grupos com distúrbio (~5000) versus sem (~7700), sugerindo que problemas de sono podem vir acompanhados de estilo de vida menos ativo (ou vice-versa, menos atividade pode prejudicar o sono).

**Diferenças por Ocupação e Gênero**

Analisando os dados por ocupação, encontramos insights interessantes:
	•	Profissionais de vendas parecem ter os piores indicadores de sono. Por exemplo, os Sales Representatives dormem em média apenas ~5.9 horas com qualidade média em torno de 4 (muito baixa) e figuram com os maiores níveis de estresse (~8 de média!). Essa ocupação pode envolver pressão por metas e horários irregulares, explicando o alto estresse e pouco sono. Outra categoria similar, Salesperson, também está entre as piores (média ~6.4h de sono, qualidade 6.0, estresse 7.0).
	•	Em contraste, ocupações de Engenharia apresentam os melhores resultados de sono. Os dados indicam que Engineers (genérico) dormem quase 8 horas (7.99h) em média, com qualidade acima de 8, e relatam o menor estresse (~3.9). Lawyers e Accountants também dormem bastante (7.4h e 7.1h) e têm qualidade alta (~7.9), possivelmente indicando rotinas mais controladas ou conscientização sobre saúde.
	•	Médicos (Doctors) e enfermeiros (Nurses) estão em posição intermediária, com ~7 horas de sono e qualidade ~7, mas curiosamente apresentam estresse relativamente elevado (especialmente médicos com média ~6.7 de estresse). Isso sugere que, embora consigam dormir uma quantidade decente, a tensão da profissão médica afeta a qualidade percebida (ainda assim melhor que a do pessoal de vendas).
	•	Professores (Teachers) e gestores (Managers) também mostraram métricas medianas (sono ~6.7-6.9h, qualidade ~7.0, estresse ~5.0), sem extremos.

Em resumo, profissões com maior estresse profissional tendem a refletir pior perfil de sono. Este achado pode ser valioso para programas de bem-estar corporativo: empresas de setores comerciais poderiam investir em apoio ao sono dos funcionários (por exemplo, gerenciamento de estresse, horários flexíveis), enquanto setores de engenharia aparentemente já demonstram equilíbrio melhor (mas não se deve ignorar indivíduos fora da média).

**Quanto às diferenças por gênero:**
	•	Mulheres no dataset dormem um pouco mais que homens (7.23 vs 7.04 horas em média) e relatam qualidade do sono superior (7.66 vs 6.97). Além disso, o nível médio de estresse das mulheres é menor (4.68 contra 6.08 dos homens).
	•	Essa tendência sugere que, nesta amostra, homens estão mais estressados e dormindo pior que mulheres. Poderia ser reflexo das ocupações (talvez mais homens em cargos de alta pressão?) ou outros fatores sociais. De qualquer forma, indica um possível público-alvo: homens poderiam demandar mais intervenções de saúde do sono.
	•	Não há diferença de gênero nos distúrbios presentes: tanto insônia quanto apneia apareceram em ambos gêneros, mas os efeitos parecem alinhados ao nível de estresse e hábitos de cada indivíduo.

**Resumo dos Insights Chave**
	•	Distúrbios do Sono são comuns: Quase 42% da amostra possui insônia ou apneia, impactando negativamente suas horas e qualidade de sono.
	•	Estresse é um fator crítico: Altos níveis de estresse se associam a sono insuficiente e insatisfatório. Programas para redução de estresse podem melhorar a saúde do sono.
	•	Atividade Física e Condicionamento: Embora todos façam alguma atividade, indivíduos com melhor sono tendem a dar mais passos diários e ter FC de repouso mais baixa (melhor condicionamento cardiovascular). Estímulo à atividade física pode trazer benefícios moderados ao sono.
	•	Ocupação influencia o sono: Profissionais de vendas destacam-se negativamente (alto estresse, pior sono), enquanto engenheiros e alguns profissionais liberais têm sono melhor. Empresas podem usar esses dados para direcionar ações de saúde ocupacional.
	•	Diferenças de Gênero: Homens da amostra tiveram piores resultados de sono que mulheres, possivelmente ligados a maiores níveis de estresse. Intervenções podem ser direcionadas levando isso em conta.

7. **Conclusão e Autoavaliação**_

Conclusão do Estudo: Construímos um pipeline completo de dados, passando pelas camadas Bronze (ingestão bruta), Silver (refinamento e limpeza) e Gold (agregação e sumarização), usando Spark no ambiente Databricks. O dataset Sleep Health and Lifestyle nos permitiu explorar como fatores de estilo de vida e saúde se relacionam com a qualidade do sono. Confirmamos expectativas, como o impacto negativo do estresse e de distúrbios como insônia, e levantamos insights acionáveis, como a identificação de grupos (por profissão ou gênero) que poderiam se beneficiar de atenção especial para melhoria do sono. Todos os passos foram documentados em detalhe, mostrando não apenas o o que foi feito, mas por que – desde decisões de modelagem até interpretação de resultados._
