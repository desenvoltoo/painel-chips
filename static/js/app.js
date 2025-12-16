/* ============================================================
   SIDEBAR
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
   DADOS DO BACKEND
============================================================ */
const chipsData = Array.isArray(window.chipsData) ? window.chipsData : [];
const aparelhosData = Array.isArray(window.aparelhosData) ? window.aparelhosData : [];


/* ============================================================
   LISTAS FIXAS
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

const OPERADORAS_LIST = ["VIVO", "TIM", "CLARO", "OI", "OUTRA"];


/* ============================================================
   HELPERS
============================================================ */
function formatDate(value) {
    if (!value) return "";

    if (typeof value === "string" && /^\d{4}-\d{2}-\d{2}$/.test(value)) {
        return value;
    }

    try {
        const d = new Date(value);
        return isNaN(d) ? "" : d.toISOString().split("T")[0];
    } catch {
        return "";
    }
}

function setValue(id, value) {
    const el = document.getElementById(id);
    if (el) el.value = value ?? "";
}


/* ============================================================
   SELECTS
============================================================ */
function preencherStatus(atual) {
    const select = document.getElementById("modal_status");
    if (!select) return;

    select.innerHTML = "";
    STATUS_LIST.forEach(s => {
        const o = document.createElement("option");
        o.value = s;
        o.textContent = s;
        if (s === atual) o.selected = true;
        select.appendChild(o);
    });
}

function preencherOperadoras(atual) {
    const select = document.getElementById("modal_operadora");
    if (!select) return;

    select.innerHTML = "";
    OPERADORAS_LIST.forEach(op => {
        const o = document.createElement("option");
        o.value = op;
        o.textContent = op;
        if (op === atual) o.selected = true;
        select.appendChild(o);
    });
}

function preencherSelectAparelhos(selecionado) {
    const select = document.getElementById("modal_sk_aparelho");
    if (!select) return;

    select.innerHTML = `<option value="">— Nenhum —</option>`;

    aparelhosData.forEach(ap => {
        const o = document.createElement("option");
        o.value = ap.sk_aparelho;
        o.textContent = `${ap.modelo} (${ap.marca})`;
        if (String(selecionado) === String(ap.sk_aparelho)) {
            o.selected = true;
        }
        select.appendChild(o);
    });
}


/* ============================================================
   RENDERIZAÇÃO DA TABELA
============================================================ */
function renderRows(lista) {
    const tbody = document.getElementById("tableBody");
    if (!tbody) return;

    tbody.innerHTML = "";

    if (!lista.length) {
        tbody.innerHTML = `
            <tr>
                <td colspan="13" class="empty-message">
                    Nenhum chip encontrado.
                </td>
            </tr>`;
        return;
    }

    lista.forEach(c => {
        const aparelho = c.aparelho_modelo
            ? `${c.aparelho_modelo}${c.aparelho_marca ? " (" + c.aparelho_marca + ")" : ""}`
            : "-";

        const obsIcon = c.observacao
            ? `<i class="fas fa-comment-dots obs-icon"
                   title="Ver observação"
                   onclick="abrirObs(${c.sk_chip})"></i>`
            : `<span class="obs-empty">—</span>`;

        const dataStatus = c.dt_inicio || c.data_inicio;

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
                <td>${aparelho}</td>
                <td>${formatDate(dataStatus)}</td>
                <td>${obsIcon}</td>
                <td>
                    <button class="btn btn-primary btn-sm edit-btn"
                        data-sk="${c.sk_chip}">
                        Editar
                    </button>
                </td>
            </tr>`;
    });

    bindEditButtons();
}


/* ============================================================
   MODAL OBSERVAÇÃO
============================================================ */
function abrirObs(sk_chip) {
    const chip = chipsData.find(c => c.sk_chip === sk_chip);
    if (!chip || !chip.observacao) return;

    document.getElementById("obsModalContent").innerText = chip.observacao;
    document.getElementById("obsModal").style.display = "flex";
}

document.getElementById("obsModalClose")?.addEventListener("click", () => {
    document.getElementById("obsModal").style.display = "none";
});


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
   MODAL DE EDIÇÃO
============================================================ */
function abrirModalEdicao(chip) {
    document.getElementById("editModal").style.display = "flex";

    setValue("modal_sk_chip", chip.sk_chip);
    setValue("modal_numero", chip.numero);
    setValue("modal_operador", chip.operador);
    setValue("modal_plano", chip.plano);
    setValue("modal_observacao", chip.observacao);

    preencherOperadoras(chip.operadora);
    preencherStatus(chip.status);

    setValue(
        "modal_data_inicio",
        formatDate(chip.dt_inicio || chip.data_inicio)
    );

    setValue("modal_ultima_recarga_data", formatDate(chip.ultima_recarga_data));
    setValue("modal_ultima_recarga_valor", chip.ultima_recarga_valor);
    setValue("modal_total_gasto", chip.total_gasto);

    preencherSelectAparelhos(chip.sk_aparelho_atual);
}


/* ============================================================
   FECHAR / SALVAR
============================================================ */
document.getElementById("modalCloseBtn")?.addEventListener("click", () => {
    document.getElementById("editModal").style.display = "none";
});

document.getElementById("modalSaveBtn")?.addEventListener("click", async () => {
    const form = new FormData(document.getElementById("modalForm"));
    const data = Object.fromEntries(form);

    // UX → modelo físico
    data.dt_inicio = data.data_inicio || null;
    delete data.data_inicio;

    data.sk_chip = document.getElementById("modal_sk_chip").value;

    const res = await fetch("/chips/update-json", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
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
