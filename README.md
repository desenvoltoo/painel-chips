📱 Painel Inteligente – Gestão de Chips & Aparelhos

Sistema corporativo para controle de chips, aparelhos, recargas, histórico e indicadores operacionais, desenvolvido com Flask + BigQuery + Cloud Run.
Todo o fluxo foi projetado para ser moderno, rápido, escalável e integrado ao ecossistema do Painel Inteligente utilizado pela universidade.

🚀 Funcionalidades Principais
🔹 Gestão de Chips

Cadastro completo

Situação (Ativo, Banido, Restrito, Pronto para maturar etc.) 

Data da última atualização

Perfil de uso (Dados / Whats / Proprietário / Perfil X)

Operadora

Armazenamento físico

Observações

Dropdown de Aparelho vinculado

🔹 Gestão de Aparelhos

Cadastro de aparelhos

Marca, modelo, tipo, operador

Status (ativo / inativo)

Dropdown inteligente usado para seleção nos chips

Histórico de uso

🔹 Vínculo Chip → Aparelho

Histórico completo de trocas

Data de início e fim

Operador responsável

Status da relação

🔹 Recargas

Registro de recargas por chip

Valor, operador, data, observação

Indicadores por período

🔹 Dashboard Operacional

Chips ativos

Chips banidos

Chips restritos

Chips sem recarga

Gráficos por operadora

Ranking de uso

Tendência de recargas

🧱 Arquitetura Técnica
Backend

Python 3.11

Flask

Rotas REST

Modularização (chips, aparelhos, recargas, relacionamento, BigQuery client)

Frontend

Bootstrap 5

DataTables

Select2

Painel responsivo e visual moderno

Banco de Dados

BigQuery – Dataset marts
Com tabelas modeladas em estrela:

⭐ Dimensões

dim_chip

dim_aparelho

⭐ Fatos

f_chip_aparelho

f_recarga

⭐ Views

vw_chips_painel

vw_aparelhos  
