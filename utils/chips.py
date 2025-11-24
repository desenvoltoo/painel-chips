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
        <option value="BLOQUEADO">BLOQUEADO</option>

    </select>

    <label>Data de Início</label>
    <input type="date" name="dt_inicio">

    <label>Última Recarga (Valor)</label>
    <input type="number" step="0.01" name="ultima_recarga_valor">

    <label>Última Recarga (Data)</label>
    <input type="date" name="ultima_recarga_data">

    <label>Total Gasto</label>
    <input type="number" step="0.01" name="total_gasto">

    <!-- Aparelho Atual, exibindo nome -->
    <label>Aparelho Atual</label>
    <select name="sk_aparelho_atual">
        <option value="">Nenhum</option>

        {% for a in aparelhos %}
            <option value="{{ a.sk_aparelho }}">
                {{ a.nome }} - {{ a.modelo }}
            </option>
        {% endfor %}
    </select>

    <button type="submit">Salvar Chip</button>
</form>



<h2>Chips Cadastrados</h2>

<table class="tabela">
<thead>
<tr>
    <th>ID</th>
    <th>Número</th>
    <th>Operadora</th>
    <th>Plano</th>
    <th>Status</th>
    <th>Última Recarga</th>
    <th>Total Gasto</th>
    <th>Aparelho Atual</th>
</tr>
</thead>
<tbody>
{% for c in chips %}
<tr>
    <td>{{ c.sk_chip }}</td>
    <td>{{ c.numero }}</td>
    <td>{{ c.operadora }}</td>
    <td>{{ c.plano }}</td>
    <td>{{ c.status }}</td>
    <td>{{ c.ultima_recarga_data }}</td>
    <td>{{ c.total_gasto }}</td>

    <!-- Nome do aparelho vindo da VIEW -->
    <td>
        {% if c.nome_aparelho %}
            {{ c.nome_aparelho }} ({{ c.modelo }})
        {% else %}
            -
        {% endif %}
    </td>

</tr>
{% endfor %}
</tbody>
</table>


<script>
document.querySelector("#form-chip").onsubmit = async e => {
    e.preventDefault();

    let data = Object.fromEntries(new FormData(e.target));

    await fetch("/chips/add", {
        method: "POST",
        body: new URLSearchParams(data)
    });

    location.reload();
};
</script>

{% endblock %}
