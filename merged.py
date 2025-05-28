import pandas as pd
import glob
import os

# --- 設定 ---
# 結合したいCSVファイル名のリスト
# 平成31年と令和元年は両方存在する可能性を考慮
target_filenames = [
    "令和4年大阪.csv",
    "令和5年大阪.csv",
    "令和6年大阪.csv"
]

# 出力する結合後のCSVファイル名
output_filename = "大阪市場日報_結合_R4_R6.csv"
# --- ここまで ---

print("CSVファイルの結合を開始します...")

# スクリプトがあるフォルダ内のCSVファイルを取得
all_csv_files = glob.glob("*.csv")

# 結合対象のファイルをフィルタリング
files_to_merge = []
found_files_set = set() # 重複を避けるため

for f in all_csv_files:
    base_name = os.path.basename(f)
    if base_name in target_filenames and base_name not in found_files_set:
        files_to_merge.append(f)
        found_files_set.add(base_name)

if not files_to_merge:
    print("エラー: 結合対象のCSVファイルが見つかりませんでした。")
    print(f"探したファイル名パターン: {target_filenames}")
    exit(1)

print("以下のファイルを結合します:")
# ファイル名でソートして表示（処理順ではない）
files_to_merge.sort() 
for f in files_to_merge:
    print(f" - {f}")

all_dataframes = []

# 各CSVファイルを読み込んでリストに追加
for file in files_to_merge:
    try:
        print(f"読み込み中: {file}")
        # ★ 必ず encoding='utf_8_sig' を指定 ★
        # これによりExcelで開いた際の文字化けを防ぎます
        df = pd.read_csv(file, encoding='utf_8_sig')
        all_dataframes.append(df)
    except FileNotFoundError:
        print(f"  -> 警告: {file} が見つかりませんでした。スキップします。")
    except Exception as e:
        print(f"  -> エラー: {file} の読み込みに失敗しました - {e}")

# データフレームを結合
if all_dataframes:
    print("\nデータフレームを結合しています...")
    # ignore_index=True で、結合後のインデックスを 0 から振り直します
    final_df = pd.concat(all_dataframes, ignore_index=True)
    print(f"結合後の総行数: {len(final_df)} 行")

    # 結合したデータを新しいCSVファイルとして保存
    try:
        # ★ 必ず encoding='utf_8_sig' を指定 ★
        final_df.to_csv(output_filename, index=False, encoding='utf_8_sig')
        print(f"\n成功: データは '{output_filename}' に保存されました。")
    except Exception as e_save:
        print(f"  -> エラー: '{output_filename}' への保存に失敗しました - {e_save}")
else:
    print("結合できるデータがありませんでした。")

print("\n--- 処理完了 ---")
print("注意: このスクリプトは平成27年から令和3年までのCSVを結合します。")
print("      令和4年分は別途処理が必要です。")