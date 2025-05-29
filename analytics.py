# analytics.py

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import japanize_matplotlib

# --- モジュールのインポート ---
from dataframe_loader import load_and_combine_market_data
from data_preprocessor import preprocess_market_data # 作成した関数をインポート

# 日本語フォント設定 (matplotlib)
try:
    # plt.rcParams['font.family'] = 'IPAexGothic' # 元の指定
    plt.rcParams['font.family'] = 'MS Gothic' # または 'Meiryo', 'Yu Gothic' など
    plt.rcParams['axes.unicode_minus'] = False
except Exception as e:
    print(f"日本語フォントの設定でエラー: {e}")

    

# --- 1. ファイルパスの設定 ---
tokyo_file_paths = ['Tokyo2014_2019.csv', 'Toyko2020_2025.csv']
sapporo_file_path = 'Sapporo2014_2025.csv'
osaka_file_path = 'Osaka2014_2025.csv'

# --- 2. データの読み込み ---
df_raw_combined = load_and_combine_market_data(tokyo_file_paths, sapporo_file_path, osaka_file_path)

# 読み込みに失敗した場合は以降の処理をスキップ
if df_raw_combined.empty:
    print("データフレームの読み込みに失敗したため、処理を終了します。")
    exit()

# --- 3. データの前処理 ---
df_all_markets = preprocess_market_data(df_raw_combined) # 前処理関数を呼び出し

# 前処理に失敗した場合は以降の処理をスキップ (preprocess_market_dataが空を返す場合など)
if df_all_markets.empty:
    print("データの前処理に失敗したか、結果が空のため、処理を終了します。")
    exit()

# ======== ここから探索的データ分析（EDA）と具体的な分析 ========
# (前回の回答のステップ9以降のコードをここに記述)

# --- 9. 単価・数量の再確認とEDA（探索的データ分析） ---
print("\n\n" + "="*20 + " ステップ9: 単価・数量の再確認とEDA " + "="*20)
quantity_col = '卸売数量_kg換算'
price_col = '単価_円perKg'
df_eda = df_all_markets.dropna(subset=[quantity_col, price_col]).copy()
if not df_eda.empty:
    print(f"\n--- {quantity_col} の基本統計量 ---")
    print(df_eda[quantity_col].describe())
    print(f"\n--- {price_col} の基本統計量 ---")
    print(df_eda[price_col].describe())
    # ... (ヒストグラム、箱ひげ図、特定魚種・市場の統計量などのEDAコード) ...
    # (前回の回答のEDA部分をここにペースト)
    # 分布の確認 (ヒストグラム)
    plt.figure(figsize=(12, 6))
    plt.subplot(1, 2, 1)
    sns.histplot(df_eda[quantity_col], bins=50, kde=False)
    plt.title(f'{quantity_col} の分布')
    plt.xlabel('数量 (kg換算)')
    plt.ylabel('頻度')
    if not df_eda[quantity_col].empty and df_eda[quantity_col].max() > 0 : plt.yscale('log')

    plt.subplot(1, 2, 2)
    sns.histplot(df_eda[price_col], bins=50, kde=False)
    plt.title(f'{price_col} の分布')
    plt.xlabel('単価 (円/kg)')
    plt.ylabel('頻度')
    if not df_eda[price_col].empty and df_eda[price_col].max() > 0 : plt.yscale('log')
    plt.tight_layout(); plt.show()

    # 外れ値の確認 (箱ひげ図)
    plt.figure(figsize=(12, 6))
    plt.subplot(1, 2, 1)
    sns.boxplot(y=df_eda[quantity_col])
    plt.title(f'{quantity_col} の箱ひげ図')
    if not df_eda[quantity_col].empty: plt.ylim(df_eda[quantity_col].quantile(0.01), df_eda[quantity_col].quantile(0.99))

    plt.subplot(1, 2, 2)
    sns.boxplot(y=df_eda[price_col])
    plt.title(f'{price_col} の箱ひげ図')
    if not df_eda[price_col].empty: plt.ylim(df_eda[price_col].quantile(0.01), df_eda[price_col].quantile(0.99))
    plt.tight_layout(); plt.show()
else:
    print(f"{quantity_col} または {price_col} が全てNaNのため、EDAプロットをスキップします。")


# --- 10. 時系列分析の準備 ---
print("\n\n" + "="*20 + " ステップ10: 時系列分析の準備 " + "="*20)
df_ts = pd.DataFrame() # 初期化
if '日付' in df_all_markets.columns and df_all_markets['日付'].notna().any():
    print("日付列をインデックスに設定します。")
    df_ts = df_all_markets.set_index('日付')
else:
    print("日付列が存在しないか、全てNaNのため、時系列分析の準備をスキップします。")

df_monthly = pd.DataFrame()
df_weekly = pd.DataFrame()
if not df_ts.empty and isinstance(df_ts.index, pd.DatetimeIndex):
    target_fish_for_ts = 'まぐろ（生鮮）' # 例
    target_market_for_ts = '豊洲'       # 例
    # ... (月次・週次データ作成コード) ...
    # (前回の回答の月次・週次データ作成部分をここにペースト)
    df_target_ts = df_ts[
        (df_ts['魚種（商品名）'] == target_fish_for_ts) &
        (df_ts['市場名_正規化'] == target_market_for_ts) &
        (df_ts[price_col].notna()) &
        (df_ts[quantity_col].notna())
    ].copy()
    if not df_target_ts.empty:
        df_monthly['月次平均単価'] = df_target_ts[price_col].resample('M').mean()
        df_monthly['月次総取引数量_kg'] = df_target_ts[quantity_col].resample('M').sum()
        df_monthly.dropna(how='all', inplace=True)

        df_weekly['週次平均単価'] = df_target_ts[price_col].resample('W').mean()
        df_weekly['週次総取引数量_kg'] = df_target_ts[quantity_col].resample('W').sum()
        df_weekly.dropna(how='all', inplace=True)
        print(f"{target_market_for_ts}市場の{target_fish_for_ts}の月次・週次データ作成完了。")
    else:
        print(f"{target_market_for_ts}市場の{target_fish_for_ts}の該当データなし。")


# --- 11. 具体的な分析テーマの実行 ---
print("\n\n" + "="*20 + " ステップ11: 具体的な分析テーマの実行 " + "="*20)
# A. 価格トレンド分析
if not df_monthly.empty and '月次平均単価' in df_monthly.columns and df_monthly['月次平均単価'].notna().any():
    # ... (価格トレンド分析コード) ...
    # (前回の回答の価格トレンド分析部分をここにペースト)
    plt.figure(figsize=(12, 6))
    df_monthly['月次平均単価'].plot()
    plt.title(f'{target_market_for_ts}市場の{target_fish_for_ts} 月次平均単価 ({price_col}) 推移')
    plt.xlabel('日付'); plt.ylabel(f'平均単価 ({price_col})'); plt.grid(True); plt.show()
else:
    print("月次平均単価データがないため、価格トレンド分析スキップ。")

# B. 市場間比較
if not df_eda.empty:
    # ... (市場間比較コード) ...
    # (前回の回答の市場間比較部分をここにペースト)
    top_fish_for_market_comparison = df_eda['魚種（商品名）'].value_counts().nlargest(3).index
    for fish in top_fish_for_market_comparison:
        df_fish_compare = df_eda[df_eda['魚種（商品名）'] == fish]
        if not df_fish_compare.empty:
            plt.figure(figsize=(10, 6))
            sns.boxplot(x='市場名_正規化', y=price_col, data=df_fish_compare, showfliers=False)
            plt.title(f'魚種「{fish}」の市場別 {price_col} 比較')
            plt.xlabel('市場'); plt.ylabel(f'{price_col}'); plt.xticks(rotation=45, ha='right'); plt.tight_layout(); plt.show()
else:
    print("EDAデータが空のため、市場間比較スキップ。")


# C. 需要分析
if not df_monthly.empty and '月次総取引数量_kg' in df_monthly.columns and df_monthly['月次総取引数量_kg'].notna().any():
    # ... (需要分析コード) ...
    # (前回の回答の需要分析部分をここにペースト)
    plt.figure(figsize=(12, 6))
    df_monthly['月次総取引数量_kg'].plot()
    plt.title(f'{target_market_for_ts}市場の{target_fish_for_ts} 月次総取引数量 ({quantity_col}) 推移')
    plt.xlabel('日付'); plt.ylabel(f'総取引数量 ({quantity_col})'); plt.grid(True); plt.show()

    if not df_eda.empty:
        print("\n--- 魚種別 総取引数量ランキング (kg換算) ---")
        print(df_eda.groupby('魚種（商品名）')[quantity_col].sum().sort_values(ascending=False).head(10))
        print("\n--- 市場別 総取引数量ランキング (kg換算) ---")
        print(df_eda.groupby('市場名_正規化')[quantity_col].sum().sort_values(ascending=False).head(10))
else:
    print("月次総取引数量データがないため、需要分析スキップ。")


# D. 相関分析
if not df_target_ts.empty and len(df_target_ts) > 1: # df_target_ts を使用
    # ... (相関分析コード) ...
    # (前回の回答の相関分析部分をここにペースト)
    plt.figure(figsize=(8, 6))
    sns.scatterplot(x=price_col, y=quantity_col, data=df_target_ts.reset_index())
    plt.title(f'{target_market_for_ts}市場の{target_fish_for_ts} - {price_col} と {quantity_col} の関係')
    plt.xlabel(f'{price_col}'); plt.ylabel(f'{quantity_col}'); plt.grid(True); plt.show()

    correlation = df_target_ts[[price_col, quantity_col]].corr()
    print(f"\n--- {target_market_for_ts}市場の{target_fish_for_ts} - {price_col} と {quantity_col} の相関係数 ---")
    print(correlation)
else:
    print(f"{target_market_for_ts}市場の{target_fish_for_ts}のデータがないか少なすぎるため、相関分析スキップ。")


print("\n\n分析処理が完了しました。")