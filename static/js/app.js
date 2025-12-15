/* ============================================================
   FORCE SIDEBAR CLOSED ON LOAD
============================================================ */
document.addEventListener("DOMContentLoaded", () => {
    const sidebar = document.getElementById("sidebar");
    const toggle = document.getElementById("sidebarToggle");

    if (sidebar && !sidebar.classList.contains("collapsed")) {
        sidebar.classList.add("collapsed");
    }

    toggle?.addEventListener("click", () => {
        sidebar.classList.toggle("collapsed");
    });
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
   FORMATAR DATA (BIGQUERY SAFE)
============================================================ */
function formatDate(value) {
    if (!value) return "-";

    try {
        // Se vier como timestamp ou string ISO
        if (typeof value === "string" && value.includes("T")) {
            return value.split("T")[0];
        }

        // Se vier como YYYY-MM-DD
        if (typeof value === "string" && /^\d{4}-\d{2}-\d{2}$/.test(value)) {
            return value;
        }

        // Fallback para Date
        const d = new Date(value);
        if (isNaN(d)) return "-";

        return d.toISOString().split("T")[0];
    } catch {
        return "-";
    }
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

    if (!lista.length) {
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
                <td>${formatDate(c.ultima_recarga_data)}</td>
                <td>${c.ultima_recarga_valor ?? "-"}</td>
                <td>${c.total_gasto ?? "-"}</td>
                <td>${c.aparelho_modelo ?? "-"}</td>
                <td>${formatDate(c.dt_inicio)}</td>
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
   BOTÃO EDITAR
============================================================ */
function bindEditButtons() {
    document.querySelectorAll(".edit-btn").forEach(btn => {
        btn.onclick = async () => {
            const sk = btn.dataset.sk;
            if (!sk) return alert("SK inválido");

            const res = await fetch(`/chips/sk/${sk}`);
            if (!res.ok) return alert("Erro ao carregar chip");

            abrirModalEdicao(await res.json());
        };
    });
}

/* ============================================================
   MODAL
============================================================ */
function abrirModalEdicao(chip) {
    document.getElementById("editModal").style.display = "flex";

    setValue("modal_sk_chip", chip.sk_chip);
    setValue("modal_numero", chip.numero);
    setValue("modal_operadora", chip.operadora);
    setValue("modal_operador", chip.operador);
    setValue("modal_plano", chip.plano);
    setValue("modal_observacao", chip.observacao);

    preencherStatus(chip.status);

    setValue("modal_dt_inicio", formatDate(chip.dt_inicio));
    setValue("modal_ultima_recarga_data", formatDate(chip.ultima_recarga_data));
    setValue("modal_ultima_recarga_valor", chip.ultima_recarga_valor);
    setValue("modal_total_gasto", chip.total_gasto);

    preencherSelectAparelhos(chip.sk_aparelho_atual);
}

/* ============================================================
   SELECT APARELHOS
============================================================ */
function preencherSelectAparelhos(selecionado) {
    const select = document.getElementById("modal_sk_aparelho_atual");
    if (!select) return;

    select.innerHTML = `<option value="">— Nenhum —</option>`;

    aparelhosData.forEach(ap => {
        const opt = document.createElement("option");
        opt.value = ap.sk_aparelho;
        opt.textContent = `${ap.modelo} (${ap.marca})`;
        if (String(selecionado) === String(ap.sk_aparelho)) opt.selected = true;
        select.appendChild(opt);
    });
}

/* ============================================================
   FECHAR / SALVAR
============================================================ */
document.getElementById("modalCloseBtn")?.addEventListener("click", () => {
    document.getElementById("editModal").style.display = "none";
});

document.getElementById("modalSaveBtn")?.addEventListener("click", async () => {
    const data = Object.fromEntries(new FormData(document.getElementById("modalForm")));

    const res = await fetch("/chips/update-json", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(data)
    });

    const r = await res.json();
    r.success ? location.reload() : alert(r.error || "Erro ao salvar");
});

/* ============================================================
   BUSCA
============================================================ */
document.getElementById("searchInput")?.addEventListener("input", e => {
    const termo = e.target.value.toLowerCase();
    renderRows(
        chipsData.filter(c =>
            Object.values(c).some(v =>
                String(v ?? "").toLowerCase().includes(termo)
            )
        )
    );
});

/* ============================================================
   INIT
============================================================ */
renderRows(chipsData);
