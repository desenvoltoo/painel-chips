from utils.bigquery_client import get_bq_client

def vincular_chip_aparelho(id_chip, id_aparelho, operador):
    sql = f"""INSERT INTO `painel-universidade.marts.f_chip_aparelho`
    (id_relacao, id_chip, id_aparelho, operador_responsavel, status_relacao)
    VALUES (GENERATE_UUID(), {id_chip}, {id_aparelho}, '{operador}', 'ativo');"""
    get_bq_client().query(sql).result()
