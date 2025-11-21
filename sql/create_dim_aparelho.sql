CREATE TABLE `painel-universidade.marts.dim_aparelho`(
id_aparelho INT64, marca STRING, model STRING, tipo STRING, status STRING,
data_cadastro TIMESTAMP DEFAULT CURRENT_TIMESTAMP());