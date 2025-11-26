# utils/sanitizer.py
import pandas as pd

def sanitize_df(df):
    if df is None or df.empty:
        return df

    # Converte tudo para string primeiro
    for col in df.columns:
        df[col] = df[col].astype("object")

    # Preenche NAs corretamente sem warning
    df = df.fillna("").infer_objects(copy=False)

    return df
