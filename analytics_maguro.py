# analytics_maguro.py

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import statsmodels.api as sm 

# --- モジュールのインポート ---
from dataframe_loader import load_and_combine_market_data
from data_preprocessor import preprocess_market_data

# 日本語フォント設定
try:
    import japanize_matplotlib
    print("japanize_matplotlib を使用して日本語フォントを設定します。")
except ImportError:
    try:
        plt.rcParams['font.family'] = 'IPAexGothic' 
        plt.rcParams['axes.unicode_minus'] = False
        print(f"Matplotlibのフォントとして {plt.rcParams['font.family']} を設定しました。")
    except Exception as e_font:
        print(f"日本語フォントの設定でエラー: {e_font}")

# --- 1. ファイルパスの設定 ---
tokyo_file_paths = ['Tokyo2014_2019.csv', 'Toyko2020_2025.csv']
sapporo_file_path = 'Sapporo2014_2025.csv'
osaka_file_path = 'Osaka2014_2025.csv'

# --- 2. データの読み込み ---
print("--- 全市場データの読み込み開始 ---")
df_raw_combined = load_and_combine_market_data(tokyo_file_paths, sapporo_file_path, osaka_file_path)
if df_raw_combined.empty: exit("データフレームの読み込み失敗")
print("--- 全市場データの読み込み完了 ---")

# --- 3. データの前処理 ---
print("\n--- 全市場データの前処理開始 ---")
df_all_markets_cleaned = preprocess_market_data(df_raw_combined)
if df_all_markets_cleaned.empty: exit("データの前処理失敗")
print("--- 全市場データの前処理完了 ---")

# --- 4. 「マグロ」関連データの抽出 (アプローチ1: 鮮度明確＋札幌・大阪特有) ---
print("\n\n" + "="*20 + " マグロ関連データの抽出 (アプローチ1) " + "="*20)
maguro_base_keywords_for_fresh_frozen = [
    'まぐろ', 'きわだ', 'きはだ', 'めばち', 'いんど', 'みなみ', 
    'まかじき', 'めかじき', 'びんちょう', 'びんなが', '本まぐろ', 'くろまぐろ'
]
sendo_keywords = ['生鮮', '冷凍']
pattern_sendo = rf"({'|'.join(sendo_keywords)})" 
pattern_maguro = rf"({'|'.join(maguro_base_keywords_for_fresh_frozen)})"

df_maguro_sendo_clear = df_all_markets_cleaned[
    df_all_markets_cleaned['魚種（商品名）'].fillna('').str.contains(pattern_sendo, case=False, na=False) &
    df_all_markets_cleaned['魚種（商品名）'].fillna('').str.contains(pattern_maguro, case=False, na=False)
].copy()
print(f"抽出された鮮度が明確なマグロ関連データの行数: {len(df_maguro_sendo_clear)}")

sapporo_main_maguro_names = ['本まぐろ', 'めばち'] 
df_sapporo_specific_maguro = df_all_markets_cleaned[
    (df_all_markets_cleaned['市場名_正規化'] == '札幌') &
    (df_all_markets_cleaned['魚種（商品名）'].isin(sapporo_main_maguro_names)) &
    (~df_all_markets_cleaned['魚種（商品名）'].fillna('').str.contains(pattern_sendo, case=False, na=False))
].copy()
print(f"抽出された札幌市場特有マグロ行数: {len(df_sapporo_specific_maguro)}")

osaka_main_maguro_names = ['くろまぐろ', 'きわだ'] 
df_osaka_specific_maguro = df_all_markets_cleaned[
    (df_all_markets_cleaned['市場名_正規化'] == '大阪（本場）') &
    (df_all_markets_cleaned['魚種（商品名）'].isin(osaka_main_maguro_names)) &
    (~df_all_markets_cleaned['魚種（商品名）'].fillna('').str.contains(pattern_sendo, case=False, na=False))
].copy()
print(f"抽出された大阪市場特有マグロ行数: {len(df_osaka_specific_maguro)}")

dfs_to_concat = []
if not df_maguro_sendo_clear.empty: dfs_to_concat.append(df_maguro_sendo_clear)
if not df_sapporo_specific_maguro.empty: dfs_to_concat.append(df_sapporo_specific_maguro)
if not df_osaka_specific_maguro.empty: dfs_to_concat.append(df_osaka_specific_maguro)

if dfs_to_concat:
    df_maguro_all = pd.concat(dfs_to_concat, ignore_index=True).drop_duplicates().reset_index(drop=True)
else:
    df_maguro_all = pd.DataFrame()
print(f"最終的なマグロ関連データの総行数: {len(df_maguro_all)}")
if df_maguro_all.empty: exit("マグロ関連データなし")

print("最終マグロ魚種（商品名）ユニーク (上位20):\n", df_maguro_all['魚種（商品名）'].value_counts().nlargest(20))
print("\n最終マグロ市場名_正規化ユニークと件数:\n", df_maguro_all['市場名_正規化'].value_counts())

quantity_col = '卸売数量_kg換算'
price_col = '単価_円perKg'
if quantity_col not in df_maguro_all.columns or price_col not in df_maguro_all.columns:
    exit(f"必須列 ({quantity_col} or {price_col}) がマグロデータに存在しません。")
if '日付' in df_maguro_all.columns and df_maguro_all['日付'].notna().any():
    df_maguro_ts = df_maguro_all.set_index('日付').copy()
else:
    df_maguro_ts = df_maguro_all.copy()

# --- デバッグ: df_maguro_all における大阪市場のデータ確認 ---
print("\n\n" + "="*20 + " デバッグ: df_maguro_all 大阪市場データ確認 " + "="*20)
df_osaka_in_maguro_all = df_maguro_all[df_maguro_all['市場名_正規化'] == '大阪（本場）'].copy()
if not df_osaka_in_maguro_all.empty:
    print(f"df_maguro_all 内の大阪市場データ件数: {len(df_osaka_in_maguro_all)}")
    print("大阪市場データの「魚種（商品名）」上位5件:")
    print(df_osaka_in_maguro_all['魚種（商品名）'].value_counts().nlargest(5))
    print("\n大阪市場データの欠損状況:")
    print(df_osaka_in_maguro_all[[quantity_col, price_col, '中値（円）', '安値（円）', '卸売数量', '数量単位_正規化']].isnull().sum())
    print("\n大阪市場データの先頭5行サンプル:")
    print(df_osaka_in_maguro_all[['魚種（商品名）', '卸売数量', '数量単位_正規化', quantity_col, '中値（円）', '安値（円）', price_col]].head())
else:
    print("df_maguro_all に大阪市場のデータなし。")
# --- デバッグここまで ---

# 日付をインデックスに設定
if '日付' in df_maguro_all.columns and df_maguro_all['日付'].notna().any():
    df_maguro_ts = df_maguro_all.set_index('日付').copy()

# --- 5. マグロデータに特化したEDA ---
print("\n\n" + "="*20 + " マグロデータのEDA " + "="*20)
df_maguro_eda = df_maguro_all.dropna(subset=[quantity_col, price_col]).copy()
print("\n--- df_maguro_eda における市場別件数 (ステップ5直後) ---")
if not df_maguro_eda.empty: print(df_maguro_eda['市場名_正規化'].value_counts(dropna=False))
else: print("df_maguro_eda は空です。")

if not df_maguro_eda.empty:
    df_maguro_eda['鮮度状態'] = '不明'
    df_maguro_eda.loc[df_maguro_eda['魚種（商品名）'].str.contains('生鮮', na=False), '鮮度状態'] = '生鮮'
    df_maguro_eda.loc[df_maguro_eda['魚種（商品名）'].str.contains('冷凍', na=False), '鮮度状態'] = '冷凍'
    print("\n鮮度状態のユニーク値と件数:\n", df_maguro_eda['鮮度状態'].value_counts(dropna=False))

    plt.figure(figsize=(12, 6))
    plt.subplot(1, 2, 1); sns.histplot(df_maguro_eda[quantity_col], bins=50, kde=False); plt.title(f'マグロ類 {quantity_col} の分布'); plt.xlabel('数量 (kg換算)'); plt.ylabel('頻度');
    if not df_maguro_eda[quantity_col].empty and df_maguro_eda[quantity_col].max() > 0: plt.yscale('log')
    plt.subplot(1, 2, 2); sns.histplot(df_maguro_eda[price_col], bins=50, kde=False); plt.title(f'マグロ類 {price_col} の分布'); plt.xlabel('単価 (円/kg)'); plt.ylabel('頻度');
    if not df_maguro_eda[price_col].empty and df_maguro_eda[price_col].max() > 0: plt.yscale('log')
    plt.tight_layout(); plt.show()

    plt.figure(figsize=(14, 7))
    order = df_maguro_eda['魚種（商品名）'].value_counts().index
    sns.boxplot(x='魚種（商品名）', y=price_col, data=df_maguro_eda, order=order, showfliers=False)
    plt.title(f'マグロ類の魚種別 {price_col} 比較'); plt.xlabel('魚種（商品名）'); plt.ylabel(f'{price_col}'); plt.xticks(rotation=60, ha='right'); plt.tight_layout(); plt.show()
    
    plt.figure(figsize=(10, 6))
    sns.boxplot(x='鮮度状態', y=price_col, data=df_maguro_eda, showfliers=False, order=['生鮮', '冷凍', '不明'])
    plt.title(f'マグロ類の鮮度状態別 {price_col} 比較'); plt.xlabel('鮮度状態'); plt.ylabel(f'{price_col}'); plt.tight_layout(); plt.show()
else:
    print(f"マグロEDAデータ ({quantity_col} or {price_col} NaNなし) が空のためEDAスキップ。")

# --- 6. 市場別分析 ---
print("\n\n" + "="*20 + " マグロデータの市場別分析 " + "="*20)
if not df_maguro_eda.empty:
    plt.figure(figsize=(12, 6))
    order_market_price = df_maguro_eda.groupby('市場名_正規化')[price_col].median().sort_values(ascending=False).index
    sns.boxplot(x='市場名_正規化', y=price_col, data=df_maguro_eda, order=order_market_price, showfliers=False)
    plt.title(f'マグロ類の市場別 {price_col} 比較'); plt.xlabel('市場'); plt.ylabel(f'{price_col}'); plt.xticks(rotation=45, ha='right'); plt.tight_layout(); plt.show()

    market_quantity_sum_maguro = df_maguro_eda.groupby('市場名_正規化')[quantity_col].sum().sort_values(ascending=False)
    print("\n--- マグロ類の市場別 総取引数量 (kg換算) ---")
    print(market_quantity_sum_maguro)
    if not market_quantity_sum_maguro.empty:
        plt.figure(figsize=(10, 6))
        market_quantity_sum_maguro.plot(kind='bar')
        plt.title(f'マグロ類の市場別 総取引数量 ({quantity_col})'); plt.xlabel('市場'); plt.ylabel(f'総取引数量 ({quantity_col})'); plt.xticks(rotation=45, ha='right'); plt.tight_layout(); plt.show()
else:
    print("マグロEDAデータが空のため市場別分析スキップ。")

# --- 7. 詳細な時系列分析 ---
print("\n\n" + "="*20 + " マグロデータの詳細時系列分析 " + "="*20)
target_maguro_fish = 'まぐろ（生鮮）' 
target_maguro_market = '東京中央' # 豊洲から東京中央へ変更
df_maguro_target_ts = pd.DataFrame() # 初期化
if not df_maguro_ts.empty and isinstance(df_maguro_ts.index, pd.DatetimeIndex):
    df_maguro_target_ts = df_maguro_ts[
        (df_maguro_ts['魚種（商品名）'].str.contains(target_maguro_fish, case=False, na=False)) &
        (df_maguro_ts['市場名_正規化'] == target_maguro_market) &
        (df_maguro_ts[price_col].notna()) &
        (df_maguro_ts[quantity_col].notna())
    ].copy()
if not df_maguro_target_ts.empty:
    df_maguro_monthly = pd.DataFrame()
    df_maguro_monthly['平均単価'] = df_maguro_target_ts[price_col].resample('M').mean()
    df_maguro_monthly['総取引数量'] = df_maguro_target_ts[quantity_col].resample('M').sum()
    df_maguro_monthly.dropna(how='all', inplace=True)
    if not df_maguro_monthly.empty:
        plt.figure(figsize=(15, 7)); df_maguro_monthly['平均単価'].plot(label='月次平均単価'); df_maguro_monthly['平均単価'].rolling(window=6).mean().plot(label='6ヶ月移動平均 (単価)');
        plt.title(f'{target_maguro_market}市場 {target_maguro_fish} {price_col} 推移'); plt.xlabel('日付'); plt.ylabel('単価'); plt.legend(); plt.grid(True); plt.show()
        plt.figure(figsize=(15, 7)); df_maguro_monthly['総取引数量'].plot(label='月次総取引数量'); df_maguro_monthly['総取引数量'].rolling(window=6).mean().plot(label='6ヶ月移動平均 (数量)');
        plt.title(f'{target_maguro_market}市場 {target_maguro_fish} {quantity_col} 推移'); plt.xlabel('日付'); plt.ylabel('数量'); plt.legend(); plt.grid(True); plt.show()
        if len(df_maguro_monthly['平均単価'].dropna()) >= 24:
            try:
                decomposition_price = sm.tsa.seasonal_decompose(df_maguro_monthly['平均単価'].dropna(), model='additive', period=12)
                fig_price_decomp = decomposition_price.plot(); fig_price_decomp.set_size_inches(12, 8)
                plt.suptitle(f'{target_maguro_market} {target_maguro_fish} 価格の季節調整', y=1.02); plt.tight_layout(); plt.show()
            except Exception as e_decomp_price: print(f"価格の季節調整中にエラー: {e_decomp_price}")
else: print(f"{target_maguro_market}市場の{target_maguro_fish}データなし、または時系列データなし")

# --- 8. 相関分析 ---
print("\n\n" + "="*20 + " マグロデータの相関分析 " + "="*20)
if not df_maguro_target_ts.empty and len(df_maguro_target_ts) > 1:
    plt.figure(figsize=(8, 6)); sns.scatterplot(x=price_col, y=quantity_col, data=df_maguro_target_ts.reset_index());
    plt.title(f'{target_maguro_market} {target_maguro_fish} - {price_col} と {quantity_col} の関係'); plt.xlabel(f'{price_col}'); plt.ylabel(f'{quantity_col}'); plt.grid(True); plt.show()
    correlation_maguro = df_maguro_target_ts[[price_col, quantity_col]].corr()
    print(f"\n--- {target_maguro_market} {target_maguro_fish} - {price_col} と {quantity_col} の相関係数 ---"); print(correlation_maguro)
else: print(f"{target_maguro_market}市場の{target_maguro_fish}データなし/少数")

print("\n\nマグロ分析処理が完了しました。")