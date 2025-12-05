/* ============================================================
   INICIAR SIDEBAR FECHADA
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
   DADOS INICIAIS DO BACKEND
============================================================ */
let chipsData = window.chipsData || [];


/* ============================================================
   FORMATAR DATA
============================================================ */
function formatDate(v) {
    if (!v) return "";
    v = String(v);
    return v.includes("T") ? v.split("T")[0] : v;
}


/* ============================================================
   RENDERIZAR TABELA
============================================================ */
function renderRows(lista) {
    const tbody = document.getElementById("tableBody");
    tbody.innerHTML = "";

    if (!lista.length) {
        tbody.innerHTML = `
            <tr><td colspan="12" class="empty-message">Nenhum chip encontrado.</td></tr>
        `;
        return;
    }

    lista.forEach(c => {
        tbody.innerHTML += `
            <tr>
                <td>${c.id_chip ?? "-"}</td>
                <td>${c.numero ?? "-"}</td>
                <td>${c.operadora ?? "-"}</td>
                <td>${c.operador ?? "-"}</td>
                <td>${c.status ?? "-"}</td>
                <td>${c.plano ?? "-"}</td>
                <td>${formatDate(c.dt_inicio) || "-"}</td>
                <td>${formatDate(c.ultima_recarga_data) || "-"}</td>
                <td>${c.ultima_recarga_valor ?? "-"}</td>
                <td>${c.total_gasto ?? "-"}</td>
                <td>${c.observacao ?? "-"}</td>

                <td>
                    <button class="btn btn-primary btn-sm edit-btn"
                            data-sk="${c.sk_chip}">
                        Editar
                    </button>
                </td>
            </tr>
        `;
    });

    bindEditButtons();
}


/* ============================================================
   BOTÃO EDITAR — CORRETO (usa SK)
============================================================ */
function bindEditButtons() {
    document.querySelectorAll(".edit-btn").forEach(btn => {
        btn.onclick = async () => {
            const sk = btn.dataset.sk;

            const res = await fetch(`/chips/sk/${sk}`);
            if (!res.ok) return alert("Erro ao buscar chip.");

            const chip = await res.json();

            document.getElementById("editModal").style.display = "flex";

            document.getElementById("modal_sk_chip").value = chip.sk_chip;
            document.getElementById("modal_numero").value = chip.numero ?? "";
            document.getElementById("modal_operadora").value = chip.operadora ?? "";
            document.getElementById("modal_operador").value = chip.operador ?? "";
            document.getElementById("modal_status").value = chip.status ?? "";
            document.getElementById("modal_plano").value = chip.plano ?? "";
            document.getElementById("modal_dt_inicio").value = formatDate(chip.dt_inicio);
            document.getElementById("modal_ultima_recarga_data").value = formatDate(chip.ultima_recarga_data);
            document.getElementById("modal_ultima_recarga_valor").value = chip.ultima_recarga_valor ?? "";
            document.getElementById("modal_total_gasto").value = chip.total_gasto ?? "";
            document.getElementById("modal_sk_aparelho_atual").value = chip.sk_aparelho_atual ?? "";
            document.getElementById("modal_observacao").value = chip.observacao ?? "";
        };
    });
}


/* ============================================================
   SALVAR ALTERAÇÕES
============================================================ */
document.getElementById("modalSaveBtn").onclick = async () => {
    const data = Object.fromEntries(new FormData(document.getElementById("modalForm")));

    const res = await fetch("/chips/update-json", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(data)
    });

    const r = await res.json();
    if (r.success) location.reload();
    else alert("Erro ao salvar.");
};


document.getElementById("modalCloseBtn").onclick = () => {
    document.getElementById("editModal").style.display = "none";
};


/* ============================================================
   BUSCA DINÂMICA
============================================================ */
document.getElementById("searchInput").addEventListener("input", e => {
    const termo = e.target.value.toLowerCase();

    const filtrados = chipsData.filter(chip =>
        Object.values(chip).some(v =>
            String(v ?? "").toLowerCase().includes(termo)
        )
    );

    renderRows(filtrados);
});


/* ============================================================
   RENDER INICIAL
============================================================ */
renderRows(chipsData);
