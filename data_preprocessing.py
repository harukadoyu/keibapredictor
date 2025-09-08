import pandas as pd
import numpy as np
import re
import unicodedata

def split_datetimes(df, cols):
    """日付: 2025.08.02 -> 年: 2025, 月: 8, 日: 2"""
    df = df.copy()
    for col in cols:
        df[col] = pd.to_datetime(df[col], format="%Y.%m.%d")
        df[col + "年"] = df[col].dt.year
        df[col + "月"] = df[col].dt.month
        df[col + "日"] = df[col].dt.day
        df.drop(col, axis=1, inplace=True)
    return df

def split_race_meeting(df, cols):
    """開催: 1札2 -> 開催回数: 1, 開催日数: 2"""

    def day_to_int(x):
            if pd.isna(x):
                return np.nan
            if x.isdigit():
                return int(x)
            return ord(x) - ord("A") + 10  # A->10, B->11, C->12, ...
    df = df.copy()
    for col in cols:
        pattern = r"(\d)(\D)(\d|[A-Z])"
        extracted = df[col].astype(str).str.extract(pattern)
        # Non JRA races has non-matching name
        df[col + "回数"] = pd.to_numeric(extracted[0], errors="coerce")
        df[col + "日数"] = extracted[2].apply(day_to_int)
        df.drop(col, axis=1, inplace=True)
    return df

def make_categorical(df, cols):
    df = df.copy()
    for col in cols:
        df[col] = pd.Categorical(df[col])
    return df

def race_class_to_numeric(df, cols):
    # FIXME "(不明)" is non-jra, nan is no previous race
    mapping = {
        "(不明)": -1, 
        "新馬": 0,
        "未勝利": 0,
        "1勝": 1,
        "500万": 1,
        "2勝": 2,
        "1000万": 2,
        "3勝": 3,
        "1600万": 3,
        "ｵｰﾌﾟﾝ": 4,
        "OP(L)": 4,
        "重賞": 5,
        "Ｇ３": 5,
        "Ｇ２": 6,
        "Ｇ１": 7,
    }
    df = df.copy()
    for col in cols:
        df[col] = df[col].map(mapping)
    return df

def condition_to_numeric(df, cols):
    df = df.copy()
    for col in cols:
        condition_map = ({'良':1, '稍':2, '重':3, '不':4})
        df[col] = df[col].map(condition_map)
    return df

def split_kinryo(df, cols):
    """55▲ -> 斤量: 55, 斤量減量: 3"""
    def parse_weight(val):
        val = str(val).strip()
        match = re.match(r"(\d+\.?\d*)", val)
        return float(match.group(1)) if match else None

    def parse_handicap(val):
        val = str(val).strip()
        match = re.search(r"[^\d\.]", val)
        symbol = match.group(0) if match else None
        mapping = {
            "★": 4.,
            "▲": 3.,
            "△": 2.,
            "☆": 2.,
            "◇": 1.,
            None: 0.
        }
        return mapping.get(symbol, 0)
    df = df.copy()
    for col in cols:
        df[col + "減量"] = df[col].apply(parse_handicap)
        df[col] = df[col].apply(parse_weight) 
    return df

def clean_finish_position(df, col, drop=True):
    """If drop is set True, drop rows with "消", "外", "止 else replace them with NaN"""
    drop_values = ["消", "外", "止"]
    # '③' -> '3', TODO: you may want to drop circled digits rows
    df = df.copy()
    df[col] = df[col].astype(str).apply(lambda x: unicodedata.normalize("NFKC", x))
    if drop:
        mask = df[col].isin(drop_values)
        dropped_count = mask.sum()
        total_rows = len(df)
        print(f"Dropped {dropped_count} rows out of {total_rows} "
              f"({dropped_count/total_rows:.2%}) where {col} is 消, 外, or 止")
        df = df[~mask]
    df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def time_columns_to_seconds(df, cols):
    """1.12.3 -> 60 + 12 + 0.3 = 72.3"""
    def time_to_seconds(t: str) -> float:
        if pd.isna(t):
            return np.nan
        m, s, d = t.split(".")
        return int(m) * 60 + int(s) + int(d) * 0.1

    df = df.copy()
    for col in cols:
        df[col] = df[col].apply(time_to_seconds)
    return df

def clean_time_columns(df, cols):
    df = df.copy()
    for col in cols:
        df[col] = df[col].replace("----", np.nan).astype(float)
    return df

def clean_tansho_column(df, col):
    def convert_val(x):
        x = str(x).strip()
        if re.match(r"^\(.*\)$", x):
            return 0
        try:
            return float(x)
        except:
            return 0

    df = df.copy()
    df[col] = df[col].apply(convert_val)
    df[col] = pd.to_numeric(df[col])  # ensure numeric type
    return df

def convert_int_columns_to_float(df):
    df = df.copy()
    int_cols = df.select_dtypes(include=["int", "int64", "int32"]).columns
    return df.astype({col: float for col in int_cols})

def rename_race_columns(df):
    df = df.copy()
    df = df.rename(columns=lambda x: x.replace("前走", "前") if x.startswith("前走") else x)
    df = df.rename(columns={"着差": "着差タイム", "前B": "前ブリンカー"})
    return df

def exclude_shogai_races(df):
    return df[df["トラックコード(JV)"] < 50].copy()

def add_last_5_races(df):
    target_cols = df.filter(like="前").columns
    df = df.sort_values(["馬名S", "日付S年", "日付S月", "日付S日"]).copy()
    # Compute shifted columns
    shifted_cols = []
    for col in target_cols:
        for i in range(1, 6):
            shifted = df.groupby("馬名S", observed=False)[col].shift(i)
            shifted.name = f"last{i}_{col}"
            shifted_cols.append(shifted)
    df = pd.concat([df] + shifted_cols, axis=1)
    
    return df

def prepare_catboost_categoricals(df, cat_cols):
    """CatBoost does not support NaN values in categorical columns.
    Therefore, missing values are replaced with the string "nan"."""
    df = df.copy()
    for col in cat_cols:
        df[col] = df[col].astype(str).fillna("nan")
    df = make_categorical(df, cat_cols)
    return df
