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
   RECEBE DADOS DO BACKEND — NÃO MODIFICAR
============================================================ */
let chipsData = window.chipsData || [];
let aparelhosData = window.aparelhosData || [];

/* ============================================================
   FORMATAR DATA
============================================================ */
function formatDate(v) {
    if (!v) return "";
    v = String(v);
    return v.includes("T") ? v.split("T")[0] : v;
}

/* ============================================================
   RENDER TABELA
============================================================ */
function renderRows(lista) {
    const tbody = document.getElementById("tableBody");
    tbody.innerHTML = "";

    if (!lista || lista.length === 0) {
        tbody.innerHTML = `<tr><td colspan="13" class="empty-message">Nenhum chip encontrado.</td></tr>`;
        return;
    }

    lista.forEach(c => {
        tbody.innerHTML += `
            <tr>
                <td>${c.id_chip ?? "-"}</td>
                <td>${c.numero ?? "-"}</td>
                <td>${c.operadora ?? "-"}</td>
                <td>${c.operador ?? "-"}</td>

                <td><span class="status-badge status-${(c.status || "").toLowerCase()}">
                    ${c.status ?? "-"}
                </span></td>

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
   FUNÇÃO PARA SETAR INPUTS
============================================================ */
function setValue(id, v) {
    const el = document.getElementById(id);
    if (el) el.value = v ?? "";
}

/* ============================================================
   BOTÃO EDITAR
============================================================ */
function bindEditButtons() {
    document.querySelectorAll(".edit-btn").forEach(btn => {
        btn.addEventListener("click", async () => {
            const sk = btn.dataset.sk;

            const res = await fetch(`/chips/sk/${sk}`);
            const chip = await res.json();

            document.getElementById("editModal").style.display = "flex";

            setValue("modal_sk_chip", chip.sk_chip);
            setValue("modal_numero", chip.numero);
            setValue("modal_operadora", chip.operadora);
            setValue("modal_operador", chip.operador);
            setValue("modal_status", chip.status);
            setValue("modal_plano", chip.plano);

            setValue("modal_dt_inicio", formatDate(chip.dt_inicio));
            setValue("modal_ultima_recarga_data", formatDate(chip.ultima_recarga_data));

            setValue("modal_ultima_recarga_valor", chip.ultima_recarga_valor);
            setValue("modal_total_gasto", chip.total_gasto);
            setValue("modal_observacao", chip.observacao);

            // aparelhos
            const select = document.getElementById("modal_sk_aparelho_atual");
            select.innerHTML = `<option value="">— Nenhum —</option>`;

            aparelhosData.forEach(ap => {
                const opt = document.createElement("option");
                opt.value = ap.sk_aparelho;
                opt.textContent = `${ap.modelo} (${ap.marca})`;

                if (chip.sk_aparelho_atual == ap.sk_aparelho) {
                    opt.selected = true;
                }

                select.appendChild(opt);
            });

        });
    });
}

/* ============================================================
   FECHAR MODAL
============================================================ */
document.getElementById("modalCloseBtn").addEventListener("click", () => {
    document.getElementById("editModal").style.display = "none";
});

/* ============================================================
   SALVAR
============================================================ */
document.getElementById("modalSaveBtn").addEventListener("click", async () => {
    const data = Object.fromEntries(new FormData(document.getElementById("modalForm")));

    const res = await fetch("/chips/update-json", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(data)
    });

    const r = await res.json();

    if (r.success) location.reload();
    else alert("Erro ao salvar.");
});

/* ============================================================
   BUSCA DINÂMICA
============================================================ */
document.getElementById("searchInput").addEventListener("input", e => {
    const termo = e.target.value.toLowerCase();

    const filtrados = chipsData.filter(chip =>
        Object.values(chip).some(val =>
            String(val ?? "").toLowerCase().includes(termo)
        )
    );

    renderRows(filtrados);
});

/* ============================================================
   RENDER INICIAL
============================================================ */
renderRows(chipsData);
