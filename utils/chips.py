{% extends "base.html" %}
{% block conteudo %}

<h2>Cadastrar Chip</h2>

<form id="form-chip" class="form-box">

    <label>ID Interno</label>
    <input name="id_chip">

    <label>Número do Chip</label>
    <input name="numero" required>

    <label>Operadora</label>
    <input name="operadora">

    <label>Plano</label>
    <input name="plano">

    <label>Status</label>
    <select name="status">

        <!-- Status operacionais -->
        <option value="DISPONIVEL">DISPONÍVEL</option>
        <option value="MATURANDO">MATURANDO</option>
        <option value="MATURADO">MATURADO</option>
        <option value="DESCANSO_1">DESCANSO 1</option>
        <option value="DESCANSO_2">DESCANSO 2</option>
        <option value="PRONTO_PARA_MATURAR">PRONTO PARA MATURAR</option>
        <option value="DISPARANDO">DISPARANDO</option>
        <option value="BANIDO">BANIDO</option>
        <option value="RESTRINGIDO">RESTRINGIDO</option>

        <!-- Antigos -->
        <option value="ATIVO">ATIVO</option>
