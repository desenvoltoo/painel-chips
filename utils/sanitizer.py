# utils/sanitizer.py
import pandas as pd

def sanitize_df(df):
    if df is None or df.empty:
        return df

    df = df.copy()

    for col in df.columns:

        # Mantém datas do BigQuery corretamente
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.strftime("%Y-%m-%d")
            continue

        # Mantém valores como estão, convertendo apenas NaN → ""
        df[col] = df[col].astype(object).where(pd.notnull(df[col]), "")

    return df
