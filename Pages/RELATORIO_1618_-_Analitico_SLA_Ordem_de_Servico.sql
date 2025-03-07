SELECT 
    ATP.NR_SEQUENCIA AS NR_ORDEM,
    ABREVIA_NOME(INITCAP(OBTER_NOME_USUARIO(ATP.NM_USUARIO_EXEC)), 'A') AS ANALISTA,
    EXTRACT(YEAR FROM ATP.DT_ORDEM_SERVICO) AS ANO,
    TO_CHAR(ATP.DT_ORDEM_SERVICO, 'Month') AS MES,
    EXTRACT(MONTH FROM ATP.DT_ORDEM_SERVICO) AS OS_MES,
    TO_CHAR(ATP.DT_ORDEM_SERVICO, 'dd/mm/yyyy hh24:mi') AS DATA,
    DECODE(ATP.IE_PRIORIDADE, 'A', 'Alta', 'M', 'Média', 'E','Emergência', 'Fora da Prioridade') AS DS_PRIORIDADE, 
    DECODE(ATP.IE_PRIORIDADE, 'A', 240, 'M', 360, 'E', 10) AS META_SLA,
    OBTER_DIF_DATA(ATP.DT_ORDEM_SERVICO, DT_FIM_REAL, 'TM') AS TEMPO_TOTAL,
    TO_CHAR(ATP.DT_INICIO_REAL ,'dd/mm/yyyy hh24:mi') AS DT_INICIO_REAL,
    TO_CHAR(ATP.DT_FIM_REAL,'dd/mm/yyyy hh24:mi') AS DT_FIM_REAL,
    CASE  
        WHEN DECODE(ATP.IE_PRIORIDADE, 'A', 240, 'M', 360, 'E', 10) >=  OBTER_DIF_DATA(ATP.DT_ORDEM_SERVICO, ATP.DT_FIM_REAL, 'TM') THEN 'Atendido'
        WHEN DECODE(ATP.IE_PRIORIDADE, 'A', 240, 'M', 360, 'E', 10) <  OBTER_DIF_DATA(ATP.DT_ORDEM_SERVICO, ATP.DT_FIM_REAL, 'TM') THEN 'Excedido'
        ELSE 'Fora do SLA'
    END AS SLA,
    SAT.CD_SETOR_ATENDIMENTO,
    INITCAP(SAT.DS_SETOR_ATENDIMENTO) AS SETOR_ATENDIMENTO,
    (
        SELECT 
            DS_TIPO
        FROM MAN_TIPO_ORDEM_SERVICO
        WHERE NR_SEQUENCIA = ATP.NR_SEQ_TIPO_ORDEM
    ) AS DESCRICAO,
    DECODE(
    --Todas=0,Aberta=1,Processo=2,Encerradas=3,
        ATP.IE_STATUS_ORDEM,
        0, 'Todas',
        1,'Aberta',
        2,'Processo',
        3,'Encerradas'
    ) AS STATUS_ORDEM    
FROM MAN_ORDEM_SERVICO ATP
INNER JOIN MAN_GRUPO_TRABALHO SA ON SA.NR_SEQUENCIA = ATP.NR_GRUPO_TRABALHO
INNER JOIN MAN_LOCALIZACAO ML ON ML.NR_SEQUENCIA = ATP.NR_SEQ_LOCALIZACAO
INNER JOIN SETOR_ATENDIMENTO SAT ON SAT.CD_SETOR_ATENDIMENTO = ML.CD_SETOR
LEFT JOIN MAN_GRUPO_PLANEJAMENTO MGP ON MGP.NR_SEQUENCIA = ATP.NR_GRUPO_PLANEJ
WHERE ATP.IE_STATUS_ORDEM = 3
AND ATP.NM_USUARIO_EXEC IN (
    'amcabral' -- Alex De Mendonca Cabral
    ,'aptsilva'-- Alexandro Pinheiro Tavares Silva
    ,'acmmeireles' --Ana Carolina Mendonca Meireles Pelegrino
    ,'iffialho' -- Ingrid Firmino Fialho
    ,'jbfilho' --João Batista Gomes De Sousa Filho
    ,'kloliveira' -- Kevin Lourenco De Oliveira
    ,'lsojunqueira' -- Lucas Souza De Oliveira Junqueira
    ,'pvplima'--	Pietro Vinicius Da Penha De Lima
    , 'ymfcastro' --YAGO MATTOS FELIPPE DE CASTRO
)
ORDER BY
    EXTRACT(YEAR FROM ATP.DT_ORDEM_SERVICO) DESC,
    EXTRACT(MONTH FROM ATP.DT_ORDEM_SERVICO) ASC,
    ATP.NR_SEQUENCIA ASC
--RETORMA APENAS A ULTIMA LINHA, APOS O GROUP BY
FETCH FIRST 10 ROWS ONLY