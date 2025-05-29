# dataframe_loader.py

import pandas as pd

def load_and_combine_market_data(tokyo_files, sapporo_file, osaka_file):
    """
    市場データを読み込み、結合して単一のDataFrameを返す関数。

    Args:
        tokyo_files (list): 東京市場のCSVファイルパスのリスト。
        sapporo_file (str): 札幌市場のCSVファイルパス。
        osaka_file (str): 大阪市場のCSVファイルパス。

    Returns:
        pandas.DataFrame: 結合された市場データ。ファイル読み込みに失敗した場合は空のDataFrame。
    """
    data_frames = []
    loaded_files_count = 0

    # 東京のデータを読み込んで結合
    tokyo_dfs = []
    print("--- 東京データの読み込み ---")
    for file in tokyo_files:
        try:
            df_temp = pd.read_csv(file, low_memory=False)
            print(f"正常に読み込みました: {file} (UTF-8 or auto-detected)")
            tokyo_dfs.append(df_temp)
            loaded_files_count +=1
        except UnicodeDecodeError:
            try:
                df_temp = pd.read_csv(file, encoding='shift-jis', low_memory=False)
                print(f"正常に読み込みました: {file} (Shift-JIS)")
                tokyo_dfs.append(df_temp)
                loaded_files_count +=1
            except FileNotFoundError:
                print(f"エラー: ファイルが見つかりません - {file}")
            except Exception as e:
                print(f"ファイルの読み込みに失敗しました ({file}): {e}")
        except FileNotFoundError:
            print(f"エラー: ファイルが見つかりません - {file}")
        except Exception as e:
            print(f"ファイルの読み込みに失敗しました ({file}): {e}")

    if tokyo_dfs:
        df_tokyo_combined = pd.concat(tokyo_dfs, ignore_index=True)
        data_frames.append(df_tokyo_combined)
        print("東京のデータを結合しました。")
    else:
        print("東京のデータファイルが1つも読み込めませんでした。")

    # 札幌のデータを読み込み
    print("\n--- 札幌データの読み込み ---")
    try:
        df_sapporo = pd.read_csv(sapporo_file, low_memory=False)
        print(f"正常に読み込みました: {sapporo_file} (UTF-8 or auto-detected)")
        data_frames.append(df_sapporo)
        loaded_files_count +=1
    except UnicodeDecodeError:
        try:
            df_sapporo = pd.read_csv(sapporo_file, encoding='shift-jis', low_memory=False)
            print(f"正常に読み込みました: {sapporo_file} (Shift-JIS)")
            data_frames.append(df_sapporo)
            loaded_files_count +=1
        except FileNotFoundError:
            print(f"エラー: ファイルが見つかりません - {sapporo_file}")
        except Exception as e:
            print(f"ファイルの読み込みに失敗しました ({sapporo_file}): {e}")
    except FileNotFoundError:
        print(f"エラー: ファイルが見つかりません - {sapporo_file}")
    except Exception as e:
        print(f"ファイルの読み込みに失敗しました ({sapporo_file}): {e}")

    # 大阪のデータを読み込み
    print("\n--- 大阪データの読み込み ---")
    try:
        df_osaka = pd.read_csv(osaka_file, low_memory=False)
        print(f"正常に読み込みました: {osaka_file} (UTF-8 or auto-detected)")
        data_frames.append(df_osaka)
        loaded_files_count +=1
    except UnicodeDecodeError:
        try:
            df_osaka = pd.read_csv(osaka_file, encoding='shift-jis', low_memory=False)
            print(f"正常に読み込みました: {osaka_file} (Shift-JIS)")
            data_frames.append(df_osaka)
            loaded_files_count +=1
        except FileNotFoundError:
            print(f"エラー: ファイルが見つかりません - {osaka_file}")
        except Exception as e:
            print(f"ファイルの読み込みに失敗しました ({osaka_file}): {e}")
    except FileNotFoundError:
        print(f"エラー: ファイルが見つかりません - {osaka_file}")
    except Exception as e:
        print(f"ファイルの読み込みに失敗しました ({osaka_file}): {e}")

    # 全てのデータフレームを結合
    if data_frames and loaded_files_count > 0:
        df_combined = pd.concat(data_frames, ignore_index=True)
        print("\n" + "="*50 + "\n")
        print(f"合計 {loaded_files_count} 個のファイルからデータを読み込み、結合しました。")
        print(f"結合後の総行数: {len(df_combined)}, 総列数: {len(df_combined.columns)}")
        print("結合後のデータの最初の数行と列名:")
        print(df_combined.head())
        print(df_combined.columns)
        return df_combined
    else:
        print("\n" + "="*50 + "\n")
        print("データファイルが一つも読み込めなかったか、結合に失敗しました。")
        return pd.DataFrame() # 空のDataFrameを返す

if __name__ == '__main__':
    # このファイル単体で実行した場合のテスト用コード
    print("dataframe_loader.py を直接実行しています（テストモード）")
    
    # テスト用のダミーファイルパス（実際のファイル名に置き換えてください）
    # これらのファイルが存在しないとエラーになります
    test_tokyo_files = ['Tokyo2014_2019.csv', 'Toyko2020_2025.csv']
    test_sapporo_file = 'Sapporo2014_2025.csv'
    test_osaka_file = 'Osaka2014_2025.csv'

    # ローカルにテスト用CSVがない場合は、以下のようにダミーファイルを作成するか、
    # 実際のファイルパスを指定してください。
    # 例:
    # pd.DataFrame({'col1': [1,2], 'col2': [3,4]}).to_csv('Tokyo2014_2019.csv', index=False)
    # pd.DataFrame({'col1': [5,6], 'col2': [7,8]}).to_csv('Toyko2020_2025.csv', index=False)
    # pd.DataFrame({'col1': [9,10], 'col2': [11,12]}).to_csv('Sapporo2014_2025.csv', index=False)
    # pd.DataFrame({'col1': [13,14], 'col2': [15,16]}).to_csv('Osaka2014_2025.csv', index=False)

    print(f"\nテスト用ファイルパス:")
    print(f"東京: {test_tokyo_files}")
    print(f"札幌: {test_sapporo_file}")
    print(f"大阪: {test_osaka_file}")
    
    print("\nテスト実行開始...")
    try:
        df_test = load_and_combine_market_data(test_tokyo_files, test_sapporo_file, test_osaka_file)
        if not df_test.empty:
            print("\nテストデータ読み込み成功。最初の5行:")
            print(df_test.head())
            print(f"\nテストデータの形状: {df_test.shape}")
        else:
            print("\nテストデータ読み込み失敗、またはデータが空です。")
    except Exception as e:
        print(f"テスト実行中にエラーが発生しました: {e}")