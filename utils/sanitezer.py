# utils/sanitizer.py

import pandas as pd

def sanitize_df(df: pd.DataFrame):
    """
    Padroniza datas, números e strings para JSON/Jinja.
    """
    for col in df.columns:

        # Datas → string
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype(str).replace("NaT", "")

        # Números
        elif pd.api.types.is_numeric_dtype(df[col]):
            df[col] = df[col].fillna(0)

        # Texto
        else:
            df[col] = df[col].fillna("")

    return df
