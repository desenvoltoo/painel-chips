/* ============================================================
   FORCE SIDEBAR CLOSED ON LOAD
============================================================ */
document.addEventListener("DOMContentLoaded", () => {
    const sidebar = document.getElementById("sidebar");
    const toggle = document.getElementById("sidebarToggle");

    if (sidebar && !sidebar.classList.contains("collapsed")) {
        sidebar.classList.add("collapsed");
    }

    if (toggle) {
        toggle.addEventListener("click", () => {
            sidebar.classList.toggle("collapsed");
        });
    }
});

/* ============================================================
   RECEBE DADOS DO BACKEND
============================================================ */
const chipsData = window.chipsData || [];
const aparelhosData = window.aparelhosData || [];

/* ============================================================
   LISTA PADRÃO DE STATUS
============================================================ */
const STATUS_LIST = [
    "DISPONIVEL",
    "MATURANDO",
    "MATURADO",
    "DESCANSO_1",
    "DESCANSO_2",
    "PRONTO_PARA_MATURAR",
    "DISPARANDO",
    "BANIDO",
    "RESTRINGIDO",
    "ATIVO",
    "BLOQUEADO"
];

/* ============================================================
   FORMATAR DATA
============================================================ */
function formatDate(value) {
    if (!value) return "";
    value = String(value);
    return value.includes("T") ? value.split("T")[0] : value;
}

/* ============================================================
   SETTER UNIVERSAL
============================================================ */
function setValue(id, value) {
    const el = document.getElementById(id);
    if (el) el.value = value ?? "";
}

/* ============================================================
   PREENCHE SELECT DE STATUS
============================================================ */
function preencherStatus(valorAtual) {
    const select = document.getElementById("modal_status");
    if (!select) return;

    select.innerHTML = "";
    STATUS_LIST.forEach(status => {
        const opt = document.createElement("option");
        opt.value = status;
        opt.textContent = status;
        if (status === valorAtual) opt.selected = true;
        select.appendChild(opt);
    });
}

/* ============================================================
   RENDERIZA TABELA DE CHIPS
============================================================ */
function renderRows(lista) {
    const tbody = document.getElementById("tableBody");
    if (!tbody) return;

    tbody.innerHTML = "";

    if (!lista?.length) {
        tbody.innerHTML = `
            <tr>
                <td colspan="13" class="empty-message">Nenhum chip encontrado.</td>
            </tr>`;
        return;
    }

    lista.forEach(c => {
        tbody.innerHTML += `
            <tr>
                <td>${c.id_chip ?? "-"}</td>
                <td>${c.numero ?? "-"}</td>
                <td>${c.operadora ?? "-"}</td>
                <td>${c.operador ?? "-"}</td>

                <td>
                    <span class="status-badge status-${(c.status || "").toLowerCase()}">
                        ${c.status ?? "-"}
                    </span>
                </td>

                <td>${c.plano ?? "-"}</td>
                <td>${formatDate(c.ultima_recarga_data) || "-"}</td>
                <td>${c.ultima_recarga_valor ?? "-"}</td>
                <td>${c.total_gasto ?? "-"}</td>
                <td>${c.modelo_aparelho ?? "-"}</td>
                <td>${formatDate(c.dt_inicio) || "-"}</td>
                <td>${c.observacao ?? "-"}</td>

                <td>
                    <button class="btn btn-primary btn-sm edit-btn" data-sk="${c.sk_chip}">
                        Editar
                    </button>
                </td>
            </tr>`;
    });

    bindEditButtons();
}

/* ============================================================
   VINCULA AÇÕES DO BOTÃO EDITAR
============================================================ */
function bindEditButtons() {
    document.querySelectorAll(".edit-btn").forEach(btn => {
        btn.addEventListener("click", async () => {
            const sk = btn.dataset.sk;
            if (!sk) return alert("Erro interno: SK inválido.");

            const res = await fetch(`/chips/sk/${sk}`);
            if (!res.ok) return alert("Erro ao carregar o chip.");

            const chip = await res.json();
            abrirModalEdicao(chip);
        });
    });
}

/* ============================================================
   ABRE MODAL + CARREGA DADOS
============================================================ */
function abrirModalEdicao(chip) {
    const modal = document.getElementById("editModal");
    if (!modal) return;
    modal.style.display = "flex";

    // BASIC
    setValue("modal_sk_chip", chip.sk_chip);
    setValue("modal_numero", chip.numero);
    setValue("modal_operadora", chip.operadora);
    setValue("modal_operador", chip.operador);
    setValue("modal_plano", chip.plano);
    setValue("modal_observacao", chip.observacao);

    // STATUS
    preencherStatus(chip.status);

    // DATAS
    setValue("modal_dt_inicio", formatDate(chip.dt_inicio));
    setValue("modal_ultima_recarga_data", formatDate(chip.ultima_recarga_data));

    // NÚMEROS
    setValue("modal_ultima_recarga_valor", chip.ultima_recarga_valor);
    setValue("modal_total_gasto", chip.total_gasto);

    // APARELHOS
    preencherSelectAparelhos(chip.sk_aparelho_atual);
}

/* ============================================================
   PREENCHE SELECT DE APARELHOS
============================================================ */
function preencherSelectAparelhos(selecionado) {
    const select = document.getElementById("modal_sk_aparelho_atual");
    if (!select) return;

    select.innerHTML = `<option value="">— Nenhum —</option>`;

    aparelhosData.forEach(ap => {
        const opt = document.createElement("option");
        opt.value = ap.sk_aparelho;
        opt.textContent = `${ap.modelo} (${ap.marca})`;

        if (selecionado == ap.sk_aparelho) opt.selected = true;
        select.appendChild(opt);
    });
}

/* ============================================================
   FECHAR MODAL
============================================================ */
document.getElementById("modalCloseBtn")?.addEventListener("click", () => {
    document.getElementById("editModal").style.display = "none";
});

/* ============================================================
   SALVAR ALTERAÇÕES
============================================================ */
document.getElementById("modalSaveBtn")?.addEventListener("click", async () => {
    const formData = Object.fromEntries(new FormData(document.getElementById("modalForm")));

    const res = await fetch("/chips/update-json", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(formData)
    });

    const r = await res.json();
    if (r.success) location.reload();
    else alert(r.error || "Erro ao salvar.");
});

/* ============================================================
   BUSCA DINÂMICA
============================================================ */
document.getElementById("searchInput")?.addEventListener("input", e => {
    const termo = e.target.value.toLowerCase();

    const filtrados = chipsData.filter(chip =>
        Object.values(chip).some(val =>
            String(val ?? "").toLowerCase().includes(termo)
        )
    );

    renderRows(filtrados);
});

/* ============================================================
   RENDERIZAÇÃO INICIAL
============================================================ */
renderRows(chipsData);
