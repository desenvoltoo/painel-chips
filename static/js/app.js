/* ============================================================
   CHIP PANEL – JS PREMIUM (Dark Blue Integration)
============================================================ */

// Dados iniciais enviados pelo backend
let chipsData = window.chipsData || [];
let aparelhosData = window.aparelhosData || [];

/* ============================================================
   BUSCA DINÂMICA
============================================================ */
document.getElementById("searchInput").addEventListener("input", e => {
    const termo = e.target.value.toLowerCase();

    const filtrados = chipsData.filter(chip =>
        Object.values(chip).some(val =>
            String(val || "").toLowerCase().includes(termo)
        )
    );

    renderRows(filtrados);
});

/* ============================================================
   RENDERIZA A TABELA
============================================================ */
function renderRows(lista) {
    const tbody = document.getElementById("tableBody");
    tbody.innerHTML = "";

    if (!lista.length) {
        tbody.innerHTML = `
            <tr>
                <td colspan="13" class="empty-message">Nenhum chip encontrado.</td>
            </tr>
        `;
        return;
    }

    lista.forEach(c => {
        tbody.innerHTML += `
            <tr>
                <td>${c.id_chip || "-"}</td>
                <td>${c.numero || "-"}</td>
                <td>${c.operadora || "-"}</td>
                <td>${c.operador || "-"}</td>

                <td>
                    <span class="status-badge status-${(c.status || '').toLowerCase()}">
                        ${c.status || "-"}
                    </span>
                </td>

                <td>${c.plano || "-"}</td>
                <td>${c.ultima_recarga_data || "-"}</td>
                <td>${c.ultima_recarga_valor || "-"}</td>
                <td>${c.total_gasto || "-"}</td>

                <td>${c.modelo_aparelho || "-"}</td>
                <td>${c.dt_inicio || "-"}</td>

                <td>
                    ${c.observacao ? `
                        <span class="obs-tooltip" data-tooltip="${c.observacao}">
                            <i class="fa fa-comment-dots"></i>
                        </span>
                    ` : "-"}
                </td>

                <td>
                    <button class="btn btn-primary btn-sm edit-btn" data-id="${c.id_chip}">
                        Editar
                    </button>
                </td>
            </tr>`;
    });

    bindEditButtons();
}

/* ============================================================
   VINCULA BOTÕES DE EDIÇÃO
============================================================ */
function bindEditButtons() {
    document.querySelectorAll(".edit-btn").forEach(btn => {
        btn.addEventListener("click", async () => {
            const id = btn.dataset.id;
            await abrirModalEdicao(id);
        });
    });
}

/* ============================================================
   ABRIR MODAL + CARREGAR DADOS
============================================================ */
async function abrirModalEdicao(id_chip) {

    const res = await fetch(`/chips/${id_chip}`);
    const chip = await res.json();

    if (!chip) return alert("Erro ao carregar dados!");

    // Mostra modal
    document.getElementById("editModal").style.display = "flex";

    /* Preenche campos */
    setValue("modal_id_chip", chip.id_chip);
    setValue("modal_numero", chip.numero);
    setValue("modal_operadora", chip.operadora);
    setValue("modal_operador", chip.operador);
    setValue("modal_status", chip.status);
    setValue("modal_plano", chip.plano);

    setValue("modal_dt_inicio", chip.dt_inicio);
    setValue("modal_ultima_recarga_data", chip.ultima_recarga_data);
    setValue("modal_ultima_recarga_valor", chip.ultima_recarga_valor);
    setValue("modal_total_gasto", chip.total_gasto);

    setValue("modal_observacao", chip.observacao);

    // Aparelhos
    const selectAp = document.getElementById("modal_sk_aparelho_atual");
    selectAp.innerHTML = `<option value="">— Nenhum —</option>`;

    aparelhosData.forEach(ap => {
        const opt = document.createElement("option");
        opt.value = ap.sk_aparelho;
        opt.textContent = `${ap.modelo} (${ap.marca})`;

        if (chip.sk_aparelho_atual == ap.sk_aparelho) opt.selected = true;
        selectAp.appendChild(opt);
    });
}

function setValue(id, value) {
    const el = document.getElementById(id);
    if (el) el.value = value || "";
}

/* ============================================================
   FECHAR MODAL
============================================================ */
document.getElementById("modalCloseBtn").addEventListener("click", () => {
    document.getElementById("editModal").style.display = "none";
});

/* ============================================================
   SALVAR ALTERAÇÕES
============================================================ */
document.getElementById("modalSaveBtn").addEventListener("click", async () => {

    const formData = Object.fromEntries(new FormData(document.getElementById("modalForm")));

    const res = await fetch("/chips/update-json", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(formData)
    });

    const r = await res.json();

    if (r.success) {
        alert("Alterações salvas com sucesso!");
        location.reload();
    } else {
        alert(r.error || "Erro ao salvar.");
    }
});

/* ============================================================
   INICIALIZAÇÃO
============================================================ */
document.addEventListener("DOMContentLoaded", () => {
    renderRows(chipsData);
});
