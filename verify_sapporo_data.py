# verify_sapporo_data.py

import pandas as pd
import numpy as np

# --- モジュールのインポート ---
from dataframe_loader import load_and_combine_market_data
from data_preprocessor import preprocess_market_data 

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


# --- デバッグ: 東京中央市場の2023年5月の「魚種（商品名）」とパターンのマッチ状況 ---
# (このデバッグブロックは、必要に応じてコメントアウトしてもOKです)
print("\n\n" + "="*20 + " デバッグ: 東京中央2023年5月 魚種名とパターンマッチ " + "="*20)
target_year_debug = 2023
target_month_debug = 5
# df_all_markets_cleaned からフィルタリング
df_tokyo_chuo_may_debug = df_all_markets_cleaned[
    (df_all_markets_cleaned['市場名_正規化'] == '東京中央') & 
    (df_all_markets_cleaned['日付'].dt.year == target_year_debug) &
    (df_all_markets_cleaned['日付'].dt.month == target_month_debug)
].copy()
if not df_tokyo_chuo_may_debug.empty:
    maguro_base_keywords_for_fresh_frozen_debug = [ 
        'まぐろ', 'きわだ', 'きはだ', 'めばち', 'いんど', 'みなみ', 
        'まかじき', 'めかじき', 'びんちょう', 'びんなが', '本まぐろ', 'くろまぐろ'
    ]
    sendo_keywords_debug = ['生鮮', '冷凍']
    pattern_sendo_debug = rf"({'|'.join(sendo_keywords_debug)})"
    pattern_maguro_debug = rf"({'|'.join(maguro_base_keywords_for_fresh_frozen_debug)})"
    # print(f"対象期間の東京中央市場データ件数: {len(df_tokyo_chuo_may_debug)}")
    # sendo_match = df_tokyo_chuo_may_debug['魚種（商品名）'].fillna('').str.contains(pattern_sendo_debug, case=False, na=False)
    # print(f"\npattern_sendo ('{pattern_sendo_debug}') にマッチする魚種:\n", df_tokyo_chuo_may_debug[sendo_match]['魚種（商品名）'].value_counts())
    # maguro_match = df_tokyo_chuo_may_debug['魚種（商品名）'].fillna('').str.contains(pattern_maguro_debug, case=False, na=False)
    # print(f"\npattern_maguro ('{pattern_maguro_debug}') にマッチする魚種:\n", df_tokyo_chuo_may_debug[maguro_match]['魚種（商品名）'].value_counts())
    # both_match = sendo_match & maguro_match
    # print(f"\npattern_sendo と pattern_maguro の両方にマッチする魚種:\n", df_tokyo_chuo_may_debug[both_match]['魚種（商品名）'].value_counts())
    # print(f"両方にマッチする件数: {both_match.sum()}")
else:
    print(f"デバッグ: {target_year_debug}年{target_month_debug}月の東京中央市場データなし。")
# --- デバッグここまで ---


# --- 4. 「マグロ」関連データの抽出 (アプローチ1: 鮮度明確＋札幌・大阪特有) ---
print("\n\n" + "="*20 + " マグロ関連データの抽出 (アプローチ1) " + "="*20)
maguro_base_keywords_for_fresh_frozen = [
    'まぐろ', 'きわだ', 'きはだ', 'めばち', 'いんど', 'みなみ', 
    'まかじき', 'めかじき', 'びんちょう', 'びんなが', '本まぐろ', 'くろまぐろ'
]
sendo_keywords = ['生鮮', '冷凍']
pattern_sendo = rf"({'|'.join(sendo_keywords)})"
pattern_maguro = rf"({'|'.join(maguro_base_keywords_for_fresh_frozen)})" # 検証3でも使うのでここで定義

df_maguro_sendo_clear = df_all_markets_cleaned[
    df_all_markets_cleaned['魚種（商品名）'].fillna('').str.contains(pattern_sendo, case=False, na=False) &
    df_all_markets_cleaned['魚種（商品名）'].fillna('').str.contains(pattern_maguro, case=False, na=False)
].copy()
sapporo_main_maguro_names = ['本まぐろ', 'めばち'] 
df_sapporo_specific_maguro = df_all_markets_cleaned[
    (df_all_markets_cleaned['市場名_正規化'] == '札幌') &
    (df_all_markets_cleaned['魚種（商品名）'].isin(sapporo_main_maguro_names)) &
    (~df_all_markets_cleaned['魚種（商品名）'].fillna('').str.contains(pattern_sendo, case=False, na=False))
].copy()
osaka_main_maguro_names = ['くろまぐろ', 'きわだ'] 
df_osaka_specific_maguro = df_all_markets_cleaned[
    (df_all_markets_cleaned['市場名_正規化'] == '大阪（本場）') &
    (df_all_markets_cleaned['魚種（商品名）'].isin(osaka_main_maguro_names)) &
    (~df_all_markets_cleaned['魚種（商品名）'].fillna('').str.contains(pattern_sendo, case=False, na=False))
].copy()

dfs_to_concat = []
if not df_maguro_sendo_clear.empty: dfs_to_concat.append(df_maguro_sendo_clear)
if not df_sapporo_specific_maguro.empty: dfs_to_concat.append(df_sapporo_specific_maguro)
if not df_osaka_specific_maguro.empty: dfs_to_concat.append(df_osaka_specific_maguro)

if dfs_to_concat:
    df_maguro_all = pd.concat(dfs_to_concat, ignore_index=True).drop_duplicates().reset_index(drop=True)
else:
    df_maguro_all = pd.DataFrame()
if df_maguro_all.empty: exit("マグロ関連データなし(検証用)")
print(f"検証用マグロデータの総行数: {len(df_maguro_all)}")

quantity_col = '卸売数量_kg換算'
price_col = '単価_円perKg'


# ======== ここから検証コード ========
# --- 検証1: 特定の月・年のデータ抽出と確認 ---
print("\n\n" + "="*20 + " 検証1: 特定月データ確認 " + "="*20)
target_year = 2023
target_month = 5 
if '日付' in df_maguro_all.columns:
    df_target_month_maguro = df_maguro_all[
        (df_maguro_all['日付'].dt.year == target_year) & (df_maguro_all['日付'].dt.month == target_month)
    ].copy()
    if not df_target_month_maguro.empty:
        print(f"\n--- {target_year}年{target_month}月のマグロ関連データ ---")
        cols_to_display = ['日付', '市場名_正規化', '魚種（商品名）', '産地', '卸売数量', '数量単位_正規化', quantity_col, price_col]
        cols_to_display = [col for col in cols_to_display if col in df_target_month_maguro.columns]
        # (各市場のサンプル表示は前回と同様のため、ここでは省略)
        print("札幌市場の該当月データ件数:", len(df_target_month_maguro[df_target_month_maguro['市場名_正規化'] == '札幌']))
        print("東京中央市場の該当月データ件数:", len(df_target_month_maguro[df_target_month_maguro['市場名_正規化'] == '東京中央']))
        print("大阪市場の該当月データ件数:", len(df_target_month_maguro[df_target_month_maguro['市場名_正規化'] == '大阪（本場）']))
    else: print(f"{target_year}年{target_month}月にマグロ関連データなし。")

# --- 検証2: 「卸売数量」と「卸売数量_kg換算」の比較検証 (札幌市場のトン換算データ) ---
print("\n\n" + "="*20 + " 検証2: トン換算データ確認 (札幌市場) " + "="*20)
original_ton_unit_col = '数量単位（トン、箱、尾など）' 
if original_ton_unit_col in df_all_markets_cleaned.columns:
    df_sapporo_ton_candidates_v2 = df_all_markets_cleaned[ # 変数名変更
        (df_all_markets_cleaned['市場名_正規化'] == '札幌') &
        (df_all_markets_cleaned[original_ton_unit_col].fillna('').str.lower().isin(['トン', 'ｔ', 't'])) &
        (df_all_markets_cleaned['魚種（商品名）'].fillna('').str.contains(pattern_maguro, case=False, na=False)) 
    ].copy()
    if not df_sapporo_ton_candidates_v2.empty:
        print(f"\n--- 札幌市場で元の単位が「トン」だったマグロデータ (最大20件) ---")
        cols_to_check_ton = ['日付', '魚種（商品名）', '卸売数量', original_ton_unit_col, '卸売数量_kg換算']
        cols_to_check_ton = [col for col in cols_to_check_ton if col in df_sapporo_ton_candidates_v2.columns]
        print(df_sapporo_ton_candidates_v2[cols_to_check_ton].head(20))
        print(f"該当候補データ件数: {len(df_sapporo_ton_candidates_v2)}")
        # トン単位取引の総量とその寄与度を計算
        total_ton_converted_kg_sapporo = df_sapporo_ton_candidates_v2['卸売数量_kg換算'].sum()
        print(f"\n札幌市場の元「トン」単位取引のkg換算後合計数量: {total_ton_converted_kg_sapporo:,.1f} kg")
        df_sapporo_maguro_for_total = df_maguro_all[
            (df_maguro_all['市場名_正規化'] == '札幌') & (df_maguro_all[quantity_col].notna()) 
        ]
        if not df_sapporo_maguro_for_total.empty:
            total_sapporo_maguro_kg_from_df_maguro_all = df_sapporo_maguro_for_total[quantity_col].sum()
            if total_sapporo_maguro_kg_from_df_maguro_all > 0:
                percentage_from_ton = (total_ton_converted_kg_sapporo / total_sapporo_maguro_kg_from_df_maguro_all) * 100
                print(f"これがdf_maguro_allから計算した札幌市場マグロ総取引量に占める割合: {percentage_from_ton:.2f}%")
    else: print("札幌市場で元の単位が「トン」だったマグロデータは見つかりませんでした(v2)。")
else: print(f"元の数量単位列 '{original_ton_unit_col}' が df_all_markets_cleaned に見つかりません。")

# --- 検証3: 札幌市場「トン」単位レコードの「卸売数量」の分布確認 ---
print("\n\n" + "="*20 + " 検証3: 札幌「トン」単位レコードの卸売数量分布 " + "="*20)
if original_ton_unit_col in df_all_markets_cleaned.columns: # original_ton_unit_col_v3 を original_ton_unit_col に統一
    df_sapporo_ton_records_v3 = df_all_markets_cleaned[ # 変数名変更
        (df_all_markets_cleaned['市場名_正規化'] == '札幌') &
        (df_all_markets_cleaned[original_ton_unit_col].fillna('').str.lower().isin(['トン', 'ｔ', 't'])) &
        (df_all_markets_cleaned['魚種（商品名）'].fillna('').str.contains(pattern_maguro, case=False, na=False)) 
    ].copy()
    if not df_sapporo_ton_records_v3.empty:
        print(f"\n--- 札幌市場で元の単位が「トン」だったマグロデータ {len(df_sapporo_ton_records_v3)}件 の「卸売数量」列の統計情報 ---")
        print(df_sapporo_ton_records_v3['卸売数量'].describe())
        print("\n--- 「卸売数量」の値が大きい上位10件 (元単位トン) ---")
        cols_to_show_ton_dist = ['日付', '魚種（商品名）', '卸売数量', original_ton_unit_col, '卸売数量_kg換算']
        cols_to_show_ton_dist = [col for col in cols_to_show_ton_dist if col in df_sapporo_ton_records_v3.columns]
        print(df_sapporo_ton_records_v3.nlargest(10, '卸売数量')[cols_to_show_ton_dist])
        print("\n--- 「卸売数量」の値が小さい上位10件 (元単位トン、0より大きいもの) ---")
        print(df_sapporo_ton_records_v3[df_sapporo_ton_records_v3['卸売数量'] > 0].nsmallest(10, '卸売数量')[cols_to_show_ton_dist])
        try:
            import matplotlib.pyplot as plt
            import seaborn as sns # seabornもインポート
            try: import japanize_matplotlib
            except ImportError:
                try: plt.rcParams['font.family'] = 'IPAexGothic'
                except: pass # フォント設定失敗は許容
            plt.figure(figsize=(10, 6)); sns.histplot(df_sapporo_ton_records_v3['卸売数量'], bins=50, kde=False)
            plt.title('札幌市場 「トン」単位レコードの「卸売数量」の分布'); plt.xlabel('卸売数量 (元データ値、単位トンと記録されたもの)'); plt.ylabel('頻度'); plt.show()
            print("「卸売数量」のヒストグラムを表示しました。")
        except ImportError: print("matplotlibまたはseabornなし。ヒストグラム表示不可。")
    else: print("札幌市場で元の単位が「トン」だったマグロデータは見つかりませんでした(v3)。")
else: print(f"元の数量単位列 '{original_ton_unit_col}' が df_all_markets_cleaned に見つかりません。")

print("\n\n検証処理が完了しました。")