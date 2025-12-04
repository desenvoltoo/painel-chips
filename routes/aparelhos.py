@aparelhos_bp.route("/aparelhos")
def aparelhos_list():
    try:
        # 1) Lista principal dos aparelhos
        df_ap = bq.get_view("vw_aparelhos")
        df_ap = sanitize_df(df_ap)

        # 2) Lista de chips (vw_chips_painel)
        #    Aqui tratamos os dois casos:
        #    - se existir id_aparelho → usa ele
        #    - se NÃO existir → usa sk_aparelho_atual
        df_chips = bq.query("""
            SELECT
                COALESCE(id_aparelho, CAST(sk_aparelho_atual AS STRING)) AS id_aparelho,
                numero,
                operadora,
                plano,
                status,
                operador,
                observacao
            FROM `painel-universidade.marts.vw_chips_painel`
            WHERE (id_aparelho IS NOT NULL OR sk_aparelho_atual IS NOT NULL)
        """)
        df_chips = sanitize_df(df_chips)

        # 3) Agrupa chips por id_aparelho
        chips_por_aparelho = {}
        for _, row in df_chips.iterrows():
            key = str(row["id_aparelho"])
            chips_por_aparelho.setdefault(key, []).append(row.to_dict())

        # 4) Renderiza com dados prontos
        return render_template(
            "aparelhos.html",
            aparelhos=df_ap.to_dict(orient="records"),
            chips_por_aparelho=chips_por_aparelho
        )

    except Exception as e:
        print("Erro ao carregar aparelhos:", e)
        return "Erro ao carregar aparelhos", 500
