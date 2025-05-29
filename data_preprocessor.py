# data_preprocessor.py

import pandas as pd
import numpy as np

def preprocess_market_data(df_initial):
    """
    市場データのクリーニングと前処理を行う関数。

    Args:
        df_initial (pandas.DataFrame): 読み込まれた生の市場データ。

    Returns:
        pandas.DataFrame: クリーニングおよび前処理済みの市場データ。
                           処理中にエラーが発生した場合は、元のDataFrameを返すか、
                           あるいは空のDataFrameを返すことも検討できます。
                           ここでは、処理を試みて、エラーがあれば部分的に処理されたものを返します。
    """
    if df_initial.empty:
        print("入力データフレームが空のため、前処理をスキップします。")
        return df_initial

    df = df_initial.copy() # 元のDataFrameを壊さないようにコピー

    print("\n\n" + "="*20 + " ステップ0: データ型再確認と日付変換 " + "="*20)
    if '日付' in df.columns:
        print("変換前の日付列のDtype:", df['日付'].dtype)
        df['日付'] = pd.to_datetime(df['日付'], errors='coerce')
        print("変換後の日付列のDtype:", df['日付'].dtype)
        na_dates_after_conversion = df['日付'].isnull().sum()
        print(f"日付変換後にNaTになった総数: {na_dates_after_conversion}")
    else:
        print("警告: '日付'列が見つかりません。")

    cols_to_numeric = ['卸売数量計', '卸売数量', '安値（円）', '中値（円）', '高値（円）', '平均価格（円）']
    print("\n数値列の型変換:")
    for col in cols_to_numeric:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            # print(f"列 '{col}' を数値型に変換しました。") # 出力が多すぎるのでコメントアウト
        else:
            print(f"警告: 数値変換対象の列 '{col}' が見つかりません。")
    # print("\n数値列の型確認後:")
    # numeric_cols_present = [c for c in cols_to_numeric if c in df.columns]
    # if numeric_cols_present:
    #     print(df[numeric_cols_present].info(verbose=False, memory_usage=False))


    print("\n\n" + "="*20 + " ステップ2: 完全な重複行の削除 " + "="*20)
    initial_rows_before_dedup = len(df)
    df.drop_duplicates(inplace=True)
    deleted_rows_dedup = initial_rows_before_dedup - len(df)
    print(f"完全な重複行を {deleted_rows_dedup}件 削除しました。")
    print(f"現在の行数: {len(df)}")


    print("\n\n" + "="*20 + " ステップ3: 「小計」行の除外 " + "="*20)
    if '魚種（商品名）' in df.columns:
        initial_rows_before_syoukei_filter = len(df)
        df = df[df['魚種（商品名）'] != '小計'].copy() # .copy()でSettingWithCopyWarningを抑制
        deleted_rows_syoukei = initial_rows_before_syoukei_filter - len(df)
        print(f"'魚種（商品名）'が「小計」である行を {deleted_rows_syoukei}件 除外しました。")
        print(f"現在の行数: {len(df)}")
    else:
        print("警告: '魚種（商品名）'列が見つかりません。「小計」行の除外はスキップします。")


    print("\n\n" + "="*20 + " ステップ4: 市場名の正規化 " + "="*20)
    if '市場名' in df.columns and '日付' in df.columns :
        # print("変更前の市場名ユニーク値と件数:\n", df['市場名'].value_counts(dropna=False))
        market_name_mapping = {
            '築地': '築地', '豊洲': '豊洲', '足立': '足立', '大田': '大田',
            '水産・築地': '築地', '水産・豊洲': '豊洲', '水産・足立': '足立', '水産・大田': '大田',
            '札幌': '札幌', '大阪': '大阪（本場）', '大阪市中央卸売市場本場': '大阪（本場）'
        }
        df['市場名_正規化'] = df['市場名'].replace(market_name_mapping)
        toyosu_start_date = pd.to_datetime('2018-10-11')
        df.loc[(df['市場名_正規化'] == '築地') & (df['日付'] >= toyosu_start_date) & (df['日付'].notna()), '市場名_正規化'] = '豊洲'
        df.loc[(df['市場名_正規化'] == '豊洲') & (df['日付'] < toyosu_start_date) & (df['日付'].notna()), '市場名_正規化'] = '築地'
        print("\n変更後の市場名_正規化 ユニーク値と件数:\n", df['市場名_正規化'].value_counts(dropna=False))
    else:
        print("警告: '市場名'列または'日付'列が見つかりません。市場名の正規化はスキップします。")
        if '市場名' in df.columns: df['市場名_正規化'] = df['市場名']


    print("\n\n" + "="*20 + " ステップ5: 数量単位の正規化 " + "="*20)
    df['数量単位_正規化'] = pd.Series(dtype='object') # FutureWarning対策
    df['卸売数量_kg換算'] = df['卸売数量'].copy() # .copy()
    primary_unit_col = '数量単位（kg、箱、尾など）'
    secondary_unit_col = '数量単位（トン、箱、尾など）'
    if primary_unit_col in df.columns:
        df[primary_unit_col] = df[primary_unit_col].astype(str).str.lower()
        kg_synonyms = ['kg', 'キロ', 'キログラム', 'ｋｇ']
        df.loc[df[primary_unit_col].isin(kg_synonyms), '数量単位_正規化'] = 'kg'
        for unit_val in ['箱', '尾', '束', '枚', 'ケース', '袋', 'パック', 'ｹｰｽ', 'p', 'CS', 'cs', '尾', 'はい', '連', 'ｶｰﾄﾝ', 'ｾｯﾄ', 'ネット', 'NETTO', 'kg以外']: # 'kg以外'も追加
            df.loc[df[primary_unit_col] == unit_val.lower(), '数量単位_正規化'] = unit_val.lower()

    if secondary_unit_col in df.columns:
        df[secondary_unit_col] = df[secondary_unit_col].astype(str).str.lower()
        kg_synonyms = ['kg', 'キロ', 'キログラム', 'ｋｇ', '1kg', '1ｋｇ'] # 1kgなども追加
        df.loc[df[secondary_unit_col].isin(kg_synonyms) & df['数量単位_正規化'].isnull(), '数量単位_正規化'] = 'kg'
        ton_synonyms = ['トン', 'ｔ', 't']
        is_ton = df[secondary_unit_col].isin(ton_synonyms) & df['数量単位_正規化'].isnull()
        df.loc[is_ton, '数量単位_正規化'] = 'kg'
        df.loc[is_ton, '卸売数量_kg換算'] = df.loc[is_ton, '卸売数量'] * 1000
        for unit_val in ['箱', '尾', '束', '枚', 'ケース', '袋', 'パック', 'ｹｰｽ', 'p', 'CS', 'cs', '尾', 'はい', '連', 'ｶｰﾄﾝ', 'ｾｯﾄ', 'ネット', 'NETTO', 'kg以外']:
             df.loc[(df[secondary_unit_col] == unit_val.lower()) & df['数量単位_正規化'].isnull(), '数量単位_正規化'] = unit_val.lower()
    df.loc[df['数量単位_正規化'] != 'kg', '卸売数量_kg換算'] = np.nan
    print("\n「数量単位_正規化」のユニーク値と件数:\n", df['数量単位_正規化'].value_counts(dropna=False))


    print("\n\n" + "="*20 + " ステップ6: 価格単位の正規化と価格調整 " + "="*20)
    price_unit_col = '価格単位（円/kg、円/箱など）'
    df['価格単位_正規化'] = pd.Series(dtype='object') # FutureWarning対策
    df['単価_円perKg'] = np.nan
    if price_unit_col in df.columns:
        df[price_unit_col] = df[price_unit_col].astype(str).str.lower()
        en_per_kg_synonyms = ['円/kg', '円/キロ', '/キロ', '円']
        is_en_per_kg = df[price_unit_col].isin(en_per_kg_synonyms)
        df.loc[is_en_per_kg, '価格単位_正規化'] = '円/kg'
        df.loc[is_en_per_kg, '単価_円perKg'] = df['中値（円）']
        en_per_ton_synonyms = ['円/トン', '円/ｔ']
        is_en_per_ton = df[price_unit_col].isin(en_per_ton_synonyms)
        df.loc[is_en_per_ton, '価格単位_正規化'] = '円/kg'
        df.loc[is_en_per_ton, '単価_円perKg'] = df['中値（円）'] / 1000
        en_per_mai_synonyms = ['円/枚']
        df.loc[df[price_unit_col].isin(en_per_mai_synonyms), '価格単位_正規化'] = '円/枚'
        for unit_suffix in ['箱', '尾', '束', 'ケース', '袋', 'パック', 'p', 'cs']:
            df.loc[df[price_unit_col] == f'円/{unit_suffix.lower()}', '価格単位_正規化'] = f'円/{unit_suffix.lower()}'
    print("\n「価格単位_正規化」のユニーク値と件数:\n", df['価格単位_正規化'].value_counts(dropna=False))


    print("\n\n" + "="*20 + " ステップ7: 主要キーでの重複の確認 " + "="*20)
    key_cols = ['日付', '市場名_正規化', '魚種（商品名）', '産地', '銘柄・規格（サイズ／グレード）', '販売方法']
    df_temp_for_dup_check = df.copy()
    for col in ['銘柄・規格（サイズ／グレード）', '販売方法', '産地', '魚種（商品名）']:
        if col in df_temp_for_dup_check.columns:
            df_temp_for_dup_check[col] = df_temp_for_dup_check[col].fillna('不明')
    valid_key_cols = [col for col in key_cols if col in df_temp_for_dup_check.columns]
    if valid_key_cols and len(valid_key_cols) == len(key_cols):
        duplicate_groups = df_temp_for_dup_check.groupby(valid_key_cols).size()
        multi_transaction_keys = duplicate_groups[duplicate_groups > 1]
        if not multi_transaction_keys.empty:
            num_multi_transaction_records = df_temp_for_dup_check.set_index(valid_key_cols).index.isin(multi_transaction_keys.index).sum()
            print(f"指定キー {valid_key_cols} は同じだが、他の項目が異なる可能性のあるレコード群が {len(multi_transaction_keys)} グループ見つかりました。")
            print(f"これらのグループに属するレコード総数は {num_multi_transaction_records} 件です。これらは個別の取引として保持します。")
        else:
            print(f"指定された主要キー {valid_key_cols} で、内容が異なる複数取引は見つかりませんでした。")


    print("\n\n" + "="*20 + " ステップ8: 不要/欠損過多列の扱い検討 " + "="*20)
    cols_to_drop = ['ID', '平均価格（円）', '備考（メモや特記事項など）', '卸売数量計']
    cols_to_drop.extend([primary_unit_col, secondary_unit_col, price_unit_col])
    if '市場名_正規化' in df.columns and '市場名' in df.columns : cols_to_drop.append('市場名')
    cols_to_drop_existing = [col for col in cols_to_drop if col in df.columns]
    if cols_to_drop_existing:
        print(f"以下の列を削除します: {cols_to_drop_existing}")
        df.drop(columns=cols_to_drop_existing, inplace=True, errors='ignore')
    print("\n削除後の列一覧:", df.columns.tolist())

    print("\nデータ前処理関数が完了しました。")
    return df

if __name__ == '__main__':
    # このファイル単体で実行した場合のテスト用コード
    print("data_preprocessor.py を直接実行しています（テストモード）")
    # ダミーデータフレームを作成してテスト
    raw_data = {
        'ID': [1, 2, 3, 3, 4, 5], '日付': ['2023/01/01', '2023/01/01', '2023/01/02', '2023/01/02', '2018/01/01', '2019/12/31'],
        '市場名': ['築地', '大阪', '札幌', '札幌', '豊洲', '築地'], '魚種（商品名）': ['まぐろ', 'さば', '小計', 'さけ', 'ぶり', 'たい'],
        '卸売数量': [100, 50, 0, 20, 30, 40], '中値（円）': [1000, 500, 0, 2000, 3000, 4000],
        '数量単位（kg、箱、尾など）': ['kg', '箱', '', 'キロ', 'kg', 'kg'],
        '数量単位（トン、箱、尾など）': [np.nan, np.nan, np.nan, np.nan, 'トン', np.nan],
        '価格単位（円/kg、円/箱など）': ['円/kg', '円/箱', '', '円/キロ', '円/トン', '円']
        # 他の列も必要に応じて追加
    }
    df_raw_test = pd.DataFrame(raw_data)
    print("\nテスト用生データ:")
    print(df_raw_test)
    
    df_processed_test = preprocess_market_data(df_raw_test.copy()) # コピーを渡す
    
    print("\nテスト用処理済みデータ:")
    if not df_processed_test.empty:
        print(df_processed_test.head())
        df_processed_test.info()
    else:
        print("処理済みデータが空です。")