/* ============================================================
   SIDEBAR
============================================================ */
document.addEventListener("DOMContentLoaded", () => {
    const sidebar = document.getElementById("sidebar");
    const toggle = document.getElementById("sidebarToggle");
    if (sidebar && !sidebar.classList.contains("collapsed")) sidebar.classList.add("collapsed");
    toggle?.addEventListener("click", () => sidebar?.classList.toggle("collapsed"));
});

/* ============================================================
   DADOS DO BACKEND
============================================================ */
const ALL_CHIPS = Array.isArray(window.chipsData) ? [...window.chipsData] : [];
let chipsView = [...ALL_CHIPS];
console.log("[Chips] Total recebido:", ALL_CHIPS.length);

const STATUS_LIST = ["DISPONIVEL", "MATURANDO", "MATURADO", "DESCANSO_1", "DESCANSO_2", "PRONTO_PARA_MATURAR", "DISPARANDO", "BANIDO", "RESTRINGIDO", "ATIVO", "EM_USO", "INATIVO", "BLOQUEADO", "MANUTENCAO"];
const OPERADORAS_LIST = ["VIVO", "TIM", "CLARO", "OI", "OUTRA"];

function formatDate(value) {
    if (!value) return "";
    const d = new Date(String(value).includes("T") ? value : `${value}T00:00:00`);
    return Number.isNaN(d.getTime()) ? "" : d.toISOString().split("T")[0];
}
function formatBRDate(value) {
    if (!value) return "—";
    const d = new Date(String(value).includes("T") ? value : `${value}T00:00:00`);
    return Number.isNaN(d.getTime()) ? "—" : d.toLocaleDateString("pt-BR");
}
function formatBRL(value) {
    const n = Number(value || 0);
    return n ? n.toLocaleString("pt-BR", { style: "currency", currency: "BRL" }) : "—";
}
function escapeHtml(value) {
    return String(value ?? "").replace(/[&<>'"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;","'":"&#39;",'"':"&quot;"}[c]));
}
function setValue(id, value) {
    const el = document.getElementById(id);
    if (el) el.value = value ?? "";
}
function notify(message, type = "info") {
    if (typeof window.showToast === "function") window.showToast(message, type);
    else console.log(`[${type}] ${message}`);
}
function showModal(id) {
    const modal = document.getElementById(id);
    if (!modal) return;
    modal.style.display = "flex";
    modal.setAttribute("aria-hidden", "false");
    document.body.classList.add("modal-open");
}
function hideModal(id) {
    const modal = document.getElementById(id);
    if (!modal) return;
    modal.style.display = "none";
    modal.setAttribute("aria-hidden", "true");
    if (!document.querySelector('.modal-overlay[style*="flex"]')) document.body.classList.remove("modal-open");
}

/* ============================================================
   MODAL NOVO CHIP
============================================================ */
function openNewChipModal() {
    const form = document.getElementById("newChipForm");
    form?.reset();
    showModal("newChipModal");
    setTimeout(() => form?.querySelector('input[name="numero"]')?.focus(), 80);
}
function closeNewChipModal() { hideModal("newChipModal"); }

document.querySelectorAll(".open-new-chip-modal").forEach(btn => btn.addEventListener("click", openNewChipModal));
document.getElementById("closeNewChipModal")?.addEventListener("click", closeNewChipModal);
document.querySelector("[data-close-new-chip]")?.addEventListener("click", closeNewChipModal);

/* ============================================================
   SELECTS E MODAL DE EDIÇÃO
============================================================ */
function preencherStatus(atual) {
    const select = document.getElementById("modal_status");
    if (!select) return;
    select.innerHTML = "";
    STATUS_LIST.forEach(s => select.appendChild(new Option(s, s, s === atual, s === atual)));
}
function preencherOperadoras(atual) {
    const select = document.getElementById("modal_operadora");
    if (!select) return;
    select.innerHTML = "";
    OPERADORAS_LIST.forEach(op => select.appendChild(new Option(op, op, op === atual, op === atual)));
}
function abrirModalEdicao(chip) {
    showModal("editModal");
    setValue("modal_sk_chip", chip.sk_chip);
    setValue("modal_numero", chip.numero);
    setValue("modal_operador", chip.operador);
    setValue("modal_plano", chip.plano);
    setValue("modal_observacao", chip.observacao);
    preencherOperadoras(chip.operadora);
    preencherStatus(chip.status);
    const statusEl = document.getElementById("modal_status");
    if (statusEl) {
        statusEl.dataset.previousStatus = chip.status || "";
        statusEl.dataset.changed = "false";
    }
    setValue("modal_qt_disparos", chip.qt_disparos);
    setValue("modal_qt_banimentos", chip.qt_banimentos);
    setValue("modal_dt_banimentos", formatDate(chip.dt_banimentos));
    setValue("modal_data_status", formatDate(chip.data_status || chip.dt_inicio));
    setValue("modal_tipo_whatsapp", chip.tipo_whatsapp);
    setValue("modal_sk_aparelho_atual", chip.sk_aparelho_atual);
    setValue("modal_slot_whatsapp", chip.slot_whatsapp);
}

function bindEditButtons() {
    document.querySelectorAll(".edit-btn").forEach(btn => {
        btn.onclick = async () => {
            const sk = btn.dataset.sk;
            if (!sk) return notify("SK inválido", "error");
            try {
                const res = await fetch(`/chips/sk/${sk}`);
                if (!res.ok) return notify("Erro ao carregar chip", "error");
                abrirModalEdicao(await res.json());
            } catch (error) {
                console.error("[Chips] Falha ao carregar chip", error);
                notify("Falha de rede ao carregar chip", "error");
            }
        };
    });
}

/* ============================================================
   RENDERIZAÇÃO DA TABELA
============================================================ */
function renderRows(lista) {
    const tbody = document.getElementById("tableBody");
    if (!tbody) return;
    if (!lista.length) {
        tbody.innerHTML = `<tr><td colspan="17" class="empty-message">Nenhum chip encontrado com os filtros aplicados.</td></tr>`;
        return;
    }
    tbody.innerHTML = lista.map(c => {
        const status = c.status || "-";
        const aparelho = c.aparelho_modelo ? `${escapeHtml(c.aparelho_modelo)} ${c.aparelho_marca ? '(' + escapeHtml(c.aparelho_marca) + ')' : ''}` : (c.sk_aparelho_atual ? `SK ${c.sk_aparelho_atual}` : "Sem aparelho");
        const rowClass = String(status).toUpperCase() === "BANIDO" ? "row-danger" : (!c.sk_aparelho_atual ? "row-warning" : "");
        return `<tr class="${rowClass}">
            <td class="quick-actions">
              <button class="action-btn edit-btn" title="Editar" data-sk="${c.sk_chip}"><i class="fas fa-edit"></i></button>
              <button class="action-btn recarga-btn" title="Recarregar" data-sk="${c.sk_chip}"><i class="fas fa-bolt"></i></button>
              <button class="action-btn banir-btn" title="Banir" data-sk="${c.sk_chip}"><i class="fas fa-ban"></i></button>
              <button class="action-btn timeline-btn" title="Histórico" data-sk="${c.sk_chip}"><i class="fas fa-clock"></i></button>
            </td>
            <td>${escapeHtml(c.numero || "—")}</td>
            <td>${escapeHtml(c.operadora || "—")}</td>
            <td><span class="status-badge status-${String(status).toLowerCase()}">${escapeHtml(status)}</span></td>
            <td>${escapeHtml(c.operador || "—")}</td>
            <td>${escapeHtml(c.plano || "—")}</td>
            <td>${escapeHtml(c.tipo_whatsapp || "—")}${c.slot_whatsapp ? ` · Slot ${escapeHtml(c.slot_whatsapp)}` : ""}</td>
            <td>${c.qt_disparos ?? 0}</td>
            <td>${c.qt_banimentos ?? 0}</td>
            <td>${formatBRDate(c.ultima_recarga_data)}<br><small>${formatBRL(c.ultima_recarga_valor)}</small></td>
            <td>${formatBRL(c.total_gasto)}</td>
            <td>${aparelho}</td>
            <td>${formatBRDate(c.updated_at || c.data_status)}</td>
            <td>${formatBRDate(c.created_at || c.dt_inicio)}</td>
            <td class="obs-cell" title="${escapeHtml(c.observacao || '')}">${c.observacao ? escapeHtml(c.observacao) : '—'}</td>
            <td>${escapeHtml(c.id_chip || "—")}</td>
            <td>${c.sk_chip ?? "—"}</td>
        </tr>`;
    }).join("");
    bindEditButtons();
    bindQuickActions();
}

function bindQuickActions() {
    document.querySelectorAll(".recarga-btn").forEach(btn => btn.onclick = async () => {
        const valor = prompt("Valor da recarga (R$):");
        if (!valor) return;
        btn.disabled = true;
        try {
            const res = await fetch("/chips/recarga", { method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({sk_chip:Number(btn.dataset.sk), valor}) });
            const out = await res.json();
            if (out.success) { notify("Recarga registrada com sucesso", "success"); setTimeout(() => location.reload(), 500); }
            else { notify(out.error || "Erro ao recarregar", "error"); btn.disabled = false; }
        } catch (e) { notify("Falha de rede ao recarregar", "error"); btn.disabled = false; }
    });
    document.querySelectorAll(".banir-btn").forEach(btn => btn.onclick = async () => {
        if (!confirm("Confirmar banimento deste chip?")) return;
        btn.disabled = true;
        try {
            const res = await fetch("/chips/banir", { method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({sk_chip:Number(btn.dataset.sk)}) });
            const out = await res.json();
            if (out.success) { notify("Chip banido", "success"); setTimeout(() => location.reload(), 500); }
            else { notify(out.error || "Erro ao banir", "error"); btn.disabled = false; }
        } catch (e) { notify("Falha de rede ao banir", "error"); btn.disabled = false; }
    });
    document.querySelectorAll(".timeline-btn").forEach(btn => btn.onclick = async () => {
        try {
            const res = await fetch(`/chips/timeline/${btn.dataset.sk}`);
            const items = await res.json();
            const box = document.getElementById("timelineContent");
            if (box) box.innerHTML = items.length ? items.map(i => `<div class="timeline-item"><strong>${escapeHtml(i.tipo_evento || 'EVENTO')}</strong><span>${formatBRDate(i.data_evento || i.created_at)}</span><p>${escapeHtml(i.observacao || '')}</p><small>${escapeHtml(i.origem || '')}</small></div>`).join("") : "<p>Nenhum histórico encontrado.</p>";
            showModal("timelineModal");
        } catch (e) { notify("Falha ao carregar histórico", "error"); }
    });
}

/* ============================================================
   FECHAR / SALVAR
============================================================ */
function closeEditModal() { hideModal("editModal"); }
document.getElementById("modalCloseBtn")?.addEventListener("click", closeEditModal);
document.getElementById("modalXCloseBtn")?.addEventListener("click", closeEditModal);
document.getElementById("timelineCloseBtn")?.addEventListener("click", () => hideModal("timelineModal"));

document.getElementById("modalSaveBtn")?.addEventListener("click", async () => {
    const btn = document.getElementById("modalSaveBtn");
    const oldText = btn.innerHTML;
    btn.disabled = true;
    btn.innerHTML = 'Salvando <span class="spinner"></span>';
    const data = {};
    new FormData(document.getElementById("modalForm")).forEach((value, key) => { data[key] = value === "" ? null : value; });
    data.sk_chip = Number(document.getElementById("modal_sk_chip").value);
    data.status = document.getElementById("modal_status").value;
    data.qt_disparos = Number(document.getElementById("modal_qt_disparos").value || 0);
    data.qt_banimentos = Number(document.getElementById("modal_qt_banimentos").value || 0);
    const statusAnterior = document.getElementById("modal_status").dataset.previousStatus || "";
    if (data.status !== statusAnterior && ["BANIDO", "BLOQUEADO", "RESTRINGIDO"].includes(data.status) && !window.confirm(`Confirmar alteração de status para "${data.status}"?`)) {
        btn.disabled = false; btn.innerHTML = oldText; return;
    }
    try {
        const res = await fetch("/chips/update-json", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(data) });
        const r = await res.json();
        if (r.success) { notify("Chip atualizado com sucesso", "success"); setTimeout(() => location.reload(), 400); }
        else notify(r.error || "Erro ao salvar", "error");
    } catch (e) {
        console.error("[Chips] Erro ao salvar", e);
        notify("Falha de rede ao salvar", "error");
    } finally {
        btn.disabled = false; btn.innerHTML = oldText;
    }
});

document.getElementById("searchInput")?.addEventListener("input", e => executarBusca(e.target.value.toLowerCase().trim()));
function executarBusca(termo) {
    chipsView = !termo ? [...ALL_CHIPS] : ALL_CHIPS.filter(c => Object.values(c).some(v => String(v ?? "").toLowerCase().includes(termo)));
    renderRows(chipsView);
}

document.addEventListener("keydown", e => {
    if (e.key !== "Escape") return;
    closeNewChipModal();
    closeEditModal();
    hideModal("timelineModal");
});
document.querySelectorAll(".modal-overlay").forEach(modal => {
    modal.addEventListener("click", event => {
        if (event.target !== modal) return;
        if (modal.id === "newChipModal") closeNewChipModal();
        else hideModal(modal.id);
    });
});

const modalStatusEl = document.getElementById("modal_status");
modalStatusEl?.addEventListener("change", () => { modalStatusEl.dataset.changed = "true"; });
renderRows(ALL_CHIPS);
