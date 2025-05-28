import pandas as pd
import glob
import os
import shutil

# --- 設定 ---
report_folder_path = './04_大阪市場日報データ（水産）'
# ★ 新しい処理対象の年 (例: 平成28年) を指定 ★
TARGET_YEAR_STR = "令和6年" 
# 処理済みフォルダ名
processed_folder_name = '処理済み'
# --- ここまで ---

def process_excel_file_layout2(file_path):
    """[main2.py用] 新しいレイアウトのExcelを処理する関数"""
    base_name = os.path.basename(file_path)
    print(f"\n--- 処理開始 (新レイアウト): {base_name} ---")
    try:
        df_full = pd.read_excel(file_path, header=None)
        print(f"  -> 読み込み完了: {df_full.shape[0]}行, {df_full.shape[1]}列")

        date_value = "日付不明"
        try:
            # ★ 日付を K1 (0, 10) から取得 ★
            if df_full.shape[0] > 0 and df_full.shape[1] > 10: # K列(10)があるかチェック
                k1_value = df_full.iloc[0, 10]
                print(f"  -> K1 (0, 10) の値: [{k1_value}]")
                if pd.notna(k1_value) and "年" in str(k1_value):
                    date_value = k1_value
                    print(f"  -> 日付取得成功 (K1): {date_value}")
                else:
                    print(f"  -> 警告: K1 で日付が見つかりません。")
            else:
                 print(f"  -> 警告: K1セル にアクセスできません。")
        except Exception as e_date:
            print(f"  -> エラー(日付取得): {base_name} - {e_date}")

        df_data = df_full.iloc[8:].copy()

        # ★★★ 新しいレイアウトの列インデックスを設定 ★★★
        # 品目(0), 数量(2), 単位(3), 高値(4), 中値(6), 安値(8), 産地1(10), 産地2(11)
        use_indices = [0, 2, 3, 4, 6, 8, 10, 11] 
        column_names_raw = ['品目', '数量', '単位', '高値', '中値', '安値', '産地1', '産地2']
        print(f"  -> フォーマット: 新レイアウト (中値あり, 産地=10+11) を使用")
        # ★★★★★★★★★★★★★★★★★★★★★★★★★★★★

        if not all(idx < df_data.shape[1] for idx in use_indices):
             print(f"  -> ★★★ エラー: {base_name} は選択しようとした列({use_indices})が不足しています。スキップします。 ★★★")
             return None, date_value

        df_raw = df_data[use_indices].copy()

        # データクリーニング
        df_raw.columns = column_names_raw
        df_clean = df_raw.dropna(subset=['数量']).copy()
        
        # ★ 産地を結合 ★
        df_clean['主な産地'] = df_clean['産地1'].fillna('') + ' ' + df_clean['産地2'].fillna('')
        df_clean['主な産地'] = df_clean['主な産地'].str.strip() # 前後の空白を削除
        # 元の産地列を削除
        df_clean = df_clean.drop(columns=['産地1', '産地2'])

        df_clean.reset_index(drop=True, inplace=True)
        df_clean['日付'] = date_value
        df_clean['元ファイル'] = base_name
        print(f"  -> 処理成功: {base_name}")
        return df_clean, date_value

    except Exception as e:
        print(f"  -> ★★★ 重大エラー: {base_name} - {e} ★★★")
        return None, "日付不明"

# --- メイン処理 ---
print(f"フォルダを検索中: '{report_folder_path}'")

processed_folder_path = os.path.join(report_folder_path, processed_folder_name)
os.makedirs(processed_folder_path, exist_ok=True)
print(f"処理済みフォルダ: '{processed_folder_path}'")

all_files_temp = glob.glob(os.path.join(report_folder_path, '*.xls')) + glob.glob(os.path.join(report_folder_path, '*.xlsx'))
excel_files = [f for f in all_files_temp if processed_folder_name not in os.path.dirname(f)]

if not excel_files:
    print(f"エラー: '{report_folder_path}' フォルダに処理対象のExcelファイルが見つかりませんでした。")
    exit(1)

print(f"ファイル {len(excel_files)} 個を更新日時でソートします...")
excel_files_sorted = sorted(excel_files, key=os.path.getmtime)
print("ソート完了。")

files_to_process = excel_files_sorted # 全ファイルを対象
print(f"{len(files_to_process)} 個のファイルを処理します...")

all_dataframes = []
failed_files = []

for file in files_to_process:
    
    # ★ 新しい関数を呼び出す ★
    processed_df, current_date_str = process_excel_file_layout2(file) 
    
    if current_date_str == "日付不明":
        print(f"  -> 警告: {os.path.basename(file)} は日付不明のため、年チェックをスキップします。")
    elif TARGET_YEAR_STR not in str(current_date_str):
        print(f"\n--- {TARGET_YEAR_STR} 以外の年 ({current_date_str}) が検出されたため、処理を停止します。 ---")
        break 

    if processed_df is not None:
        all_dataframes.append(processed_df)
        try:
            dest_path = os.path.join(processed_folder_path, os.path.basename(file))
            print(f"  -> 移動中: {os.path.basename(file)} -> {processed_folder_name}/")
            shutil.move(file, dest_path)
        except Exception as e_move:
            print(f"  -> ★★★ エラー(移動失敗): {os.path.basename(file)} - {e_move} ★★★")
            failed_files.append(f"{os.path.basename(file)} (移動失敗)")
    else:
        failed_files.append(os.path.basename(file))

# --- 結果の結合と表示 ---
if all_dataframes:
    print("\n--- 全データの結合 ---")
    final_df = pd.concat(all_dataframes, ignore_index=True)

    print(f"結合後の総行数: {len(final_df)} 行")
    print("\n--- 結合後のデータ (最初の5行) ---")
    print(final_df.head())
    print("\n--- 結合後のデータ (最後の5行) ---")
    print(final_df.tail())
    print("\n--- 結合後のデータ情報 ---")
    final_df.info()

    # --- 最終結果を保存 (CSV形式) ---
    output_filename = f"{TARGET_YEAR_STR}大阪.csv" 
    try:
        final_df.to_csv(output_filename, index=False, encoding='utf_8_sig') 
        print(f"\n--- ★★★ 最終結果を {output_filename} に保存しました ★★★ ---")
    except Exception as e_save:
        print(f"\n--- ★★★ エラー(最終保存失敗): {e_save} ★★★ ---")

else:
    print("\n正常に処理できたファイルがありませんでした。")

if failed_files:
    print("\n--- 以下のファイルでエラーまたは移動失敗が発生しました ---")
    for f_name in failed_files:
        print(f"- {f_name}")