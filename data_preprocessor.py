# data_preprocessor.py

import pandas as pd
import numpy as np

def preprocess_market_data(df_initial):
    """
    市場データのクリーニングと前処理を行う関数。
    """
    if df_initial.empty:
        print("入力データフレームが空のため、前処理をスキップします。")
        return df_initial

    df = df_initial.copy()

    print("\n\n" + "="*20 + " ステップ0: データ型再確認と日付変換 " + "="*20)
    if '日付' in df.columns:
        df['日付'] = pd.to_datetime(df['日付'], errors='coerce')
    cols_to_numeric = ['卸売数量計', '卸売数量', '安値（円）', '中値（円）', '高値（円）', '平均価格（円）']
    for col in cols_to_numeric:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    print("\n\n" + "="*20 + " ステップ2: 完全な重複行の削除 " + "="*20)
    initial_rows_before_dedup = len(df)
    df.drop_duplicates(inplace=True)
    print(f"完全な重複行を {initial_rows_before_dedup - len(df)}件 削除しました。 現在行数: {len(df)}")

    print("\n\n" + "="*20 + " ステップ3: 「小計」行の除外 " + "="*20)
    if '魚種（商品名）' in df.columns:
        initial_rows_before_syoukei_filter = len(df)
        df = df[df['魚種（商品名）'] != '小計'].copy()
        print(f"'魚種（商品名）'が「小計」である行を {initial_rows_before_syoukei_filter - len(df)}件 除外。 現在行数: {len(df)}")

    print("\n\n" + "="*20 + " ステップ4: 市場名の正規化 (東京市場統一) " + "="*20)
    if '市場名' in df.columns and '日付' in df.columns :
        market_name_initial_mapping = {
            '水産・築地': '築地', '水産・豊洲': '豊洲', '水産・足立': '足立', '水産・大田': '大田',
            '大阪市中央卸売市場本場': '大阪'
        }
        df['市場名_temp'] = df['市場名'].replace(market_name_initial_mapping)
        toyosu_start_date = pd.to_datetime('2018-10-11')
        df['市場名_正規化'] = df['市場名_temp']
        df.loc[df['市場名_temp'] == '築地', '市場名_正規化'] = '東京中央'
        df.loc[df['市場名_temp'] == '豊洲', '市場名_正規化'] = '東京中央'
        df.loc[df['市場名_temp'] == '大阪', '市場名_正規化'] = '大阪（本場）'
        df.drop(columns=['市場名_temp'], inplace=True)
        print("最終的な市場名_正規化 ユニーク値と件数:\n", df['市場名_正規化'].value_counts(dropna=False))

    print("\n\n" + "="*20 + " ステップ5: 数量単位の正規化 " + "="*20)
    # --- デバッグ用出力（必要に応じてコメント解除） ---
    # if '市場名_正規化' in df.columns:
    #     df_tokyo_chuo_debug = df[df['市場名_正規化'] == '東京中央']
    #     if not df_tokyo_chuo_debug.empty:
    #         if '数量単位（kg、箱、尾など）' in df_tokyo_chuo_debug.columns:
    #             print("\n--- (デバッグ) 東京中央市場の「数量単位（kg、箱、尾など）」上位10件 ---")
    #             print(df_tokyo_chuo_debug['数量単位（kg、箱、尾など）'].value_counts(dropna=False).nlargest(10))
    #         if '数量単位（トン、箱、尾など）' in df_tokyo_chuo_debug.columns:
    #             print("\n--- (デバッグ) 東京中央市場の「数量単位（トン、箱、尾など）」上位10件 ---")
    #             print(df_tokyo_chuo_debug['数量単位（トン、箱、尾など）'].value_counts(dropna=False).nlargest(10))

    df['数量単位_正規化'] = pd.Series(dtype='object') 
    df['卸売数量_kg換算'] = df['卸売数量'].copy() 
    primary_unit_col = '数量単位（kg、箱、尾など）'
    secondary_unit_col = '数量単位（トン、箱、尾など）'
    if primary_unit_col in df.columns: df[primary_unit_col] = df[primary_unit_col].astype(str).str.lower()
    if secondary_unit_col in df.columns: df[secondary_unit_col] = df[secondary_unit_col].astype(str).str.lower()
    
    if primary_unit_col in df.columns:
        kg_synonyms_primary = ['kg', 'キロ', 'キログラム', 'ｋｇ', 'キログラム(kg)', 'ｋｇ(キログラム)']
        df.loc[df[primary_unit_col].isin(kg_synonyms_primary), '数量単位_正規化'] = 'kg'
        other_units_primary = ['箱', '尾', '束', '枚', 'ケース', '袋', 'パック', 'ｹｰｽ', 'p', 'CS', 'cs', 'はい', '連', 'ｶｰﾄﾝ', 'ｾｯﾄ', 'ネット', 'NETTO', 'kg以外', 'ｶｺﾞ']
        for unit_val in other_units_primary: df.loc[df[primary_unit_col] == unit_val, '数量単位_正規化'] = unit_val
    
    if secondary_unit_col in df.columns:
        kg_synonyms_secondary = ['kg', 'キロ', 'キログラム', 'ｋｇ', '1kg', '1ｋｇ', '1キログラム', '1ｋｇ(キログラム)', '1ｋｇ']
        condition_kg_secondary = df[secondary_unit_col].isin(kg_synonyms_secondary) & df['数量単位_正規化'].isnull()
        df.loc[condition_kg_secondary, '数量単位_正規化'] = 'kg'
        ton_synonyms = ['トン', 'ｔ', 't']
        condition_ton_secondary = df[secondary_unit_col].isin(ton_synonyms) & df['数量単位_正規化'].isnull()
        df.loc[condition_ton_secondary, '数量単位_正規化'] = 'kg'
        df.loc[condition_ton_secondary, '卸売数量_kg換算'] = df.loc[condition_ton_secondary, '卸売数量'] * 1000
        other_units_secondary = ['箱', '尾', '束', '枚', 'ケース', '袋', 'パック', 'ｹｰｽ', 'p', 'CS', 'cs', 'はい', '連', 'ｶｰﾄﾝ', 'ｾｯﾄ', 'ネット', 'NETTO', 'kg以外', 'ｶｺﾞ']
        for unit_val in other_units_secondary:
            condition_other_secondary = (df[secondary_unit_col] == unit_val) & df['数量単位_正規化'].isnull()
            df.loc[condition_other_secondary, '数量単位_正規化'] = unit_val
            
    condition_default_kg = df['数量単位_正規化'].isnull()
    if primary_unit_col in df.columns: condition_default_kg &= (df[primary_unit_col].isin(['nan', 'NaN', '']) )
    if secondary_unit_col in df.columns: condition_default_kg &= (df[secondary_unit_col].isin(['nan', 'NaN', '']) )
    if '卸売数量' in df.columns: condition_default_kg &= df['卸売数量'].notna() & (df['卸売数量'] > 0)
    df.loc[condition_default_kg, '数量単位_正規化'] = 'kg'
    df.loc[df['数量単位_正規化'] != 'kg', '卸売数量_kg換算'] = np.nan
    print("「数量単位_正規化」ユニーク値と件数 (修正後):\n", df['数量単位_正規化'].value_counts(dropna=False))

    print("\n\n" + "="*20 + " ステップ6: 価格単位の正規化と価格調整 " + "="*20)
    
    # --- (デバッグ用: 大阪市場の実際の「価格単位」を確認 はそのまま) ---
    if '市場名_正規化' in df.columns and '魚種（商品名）' in df.columns and '価格単位（円/kg、円/箱など）' in df.columns:
        df_osaka_price_unit_actual_debug = df[
            (df['市場名_正規化'] == '大阪（本場）') &
            (df['魚種（商品名）'].isin(['くろまぐろ', 'きわだ']))
        ]
        if not df_osaka_price_unit_actual_debug.empty:
            print("\n--- (デバッグ) 大阪市場「くろまぐろ」「きわだ」の実際の「価格単位（円/kg、円/箱など）」 ---")
            print(df_osaka_price_unit_actual_debug['価格単位（円/kg、円/箱など）'].value_counts(dropna=False))
    # --- デバッグここまで ---

    price_unit_col = '価格単位（円/kg、円/箱など）'
    df['価格単位_正規化'] = pd.Series(dtype='object')
    df['単価_円perKg'] = np.nan 

    if price_unit_col in df.columns:
        df[price_unit_col] = df[price_unit_col].astype(str).str.lower() # まず小文字化
        
        # 円/kg 系統の同義語リスト
        en_per_kg_synonyms = ['円/kg', '円/キロ', '/キロ', '円']
        
        # ★★★ 大阪市場の価格単位NaNを円/kgとみなす条件を追加 ★★★
        condition_osaka_price_unit_nan = (
            (df['市場名_正規化'] == '大阪（本場）') &
            (df['魚種（商品名）'].isin(['くろまぐろ', 'きわだ'])) &
            (df[price_unit_col].isin(['nan', 'NaN', ''])) # 価格単位が実質的に空の場合
        )
        # ★★★ ここまで ★★★

        # is_en_per_kg の条件を更新
        is_en_per_kg = df[price_unit_col].isin(en_per_kg_synonyms) | condition_osaka_price_unit_nan
        
        df.loc[is_en_per_kg, '価格単位_正規化'] = '円/kg'
        
        df.loc[is_en_per_kg, '単価_円perKg'] = np.where(
            df.loc[is_en_per_kg, '中値（円）'].notna(),       
            df.loc[is_en_per_kg, '中値（円）'],              
            np.where(
                df.loc[is_en_per_kg, '安値（円）'].notna(),   
                df.loc[is_en_per_kg, '安値（円）'],          
                np.nan                                    
            )
        )

        # 円/トン 系統の処理 (これは変更なし)
        en_per_ton_synonyms = ['円/トン', '円/ｔ']
        is_en_per_ton = df[price_unit_col].isin(en_per_ton_synonyms) # 大阪のNaNケースは既にis_en_per_kgで処理されるので、ここはそのまま
        df.loc[is_en_per_ton, '価格単位_正規化'] = '円/kg' 
        df.loc[is_en_per_ton, '単価_円perKg'] = np.where(
            df.loc[is_en_per_ton, '中値（円）'].notna(),
            df.loc[is_en_per_ton, '中値（円）'] / 1000,
            np.where(
                df.loc[is_en_per_ton, '安値（円）'].notna(),
                df.loc[is_en_per_ton, '安値（円）'] / 1000,
                np.nan
            )
        )
        
        en_per_mai_synonyms = ['円/枚']
        df.loc[df[price_unit_col].isin(en_per_mai_synonyms), '価格単位_正規化'] = '円/枚'
        for unit_suffix in ['箱', '尾', '束', 'ケース', '袋', 'パック', 'p', 'cs']:
            df.loc[df[price_unit_col] == f'円/{unit_suffix.lower()}', '価格単位_正規化'] = f'円/{unit_suffix.lower()}'

    print("\n「価格単位_正規化」のユニーク値と件数 (修正後):")
    print(df['価格単位_正規化'].value_counts(dropna=False))
    print("\n「単価_円perKg」の欠損数 (修正後):")
    print(df['単価_円perKg'].isnull().sum())

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
            print(f"指定キーでの複数取引レコード群: {len(multi_transaction_keys)} グループ, {num_multi_transaction_records} 件")

    print("\n\n" + "="*20 + " ステップ8: 不要/欠損過多列の扱い検討 " + "="*20)
    cols_to_drop = ['ID', '平均価格（円）', '備考（メモや特記事項など）', '卸売数量計']
    # secondary_unit_col を残すために、以下の行を修正
    cols_to_drop.extend([primary_unit_col, price_unit_col]) # secondary_unit_col を削除リストから除外
    if '市場名_正規化' in df.columns and '市場名' in df.columns : cols_to_drop.append('市場名')
    cols_to_drop_existing = [col for col in cols_to_drop if col in df.columns]
    if cols_to_drop_existing:
        df.drop(columns=cols_to_drop_existing, inplace=True, errors='ignore')
    print("削除後の列一覧:", df.columns.tolist())

    print("\nデータ前処理関数が完了しました。")
    return df

if __name__ == '__main__':
    # (テストコードは変更なし)
    print("data_preprocessor.py を直接実行しています（テストモード）")
    raw_data = {
        'ID': [1,2,3,3,4,5,6], '日付': ['2023/01/01','2023/01/01','2023/01/02','2023/01/02','2018/01/01','2019/12/31','2023/01/03'],
        '市場名': ['築地','大阪','札幌','札幌','豊洲','築地','大阪（本場）'], '魚種（商品名）': ['まぐろ','さば','小計','さけ','ぶり','たい','くろまぐろ'],
        '卸売数量': [100,50,0,20,30,40,60], '中値（円）': [1000,np.nan,0,2000,3000,4000,np.nan], '安値（円）': [900,450,0,1900,2900,3900,5000],
        '数量単位（kg、箱、尾など）': ['kg','箱','','キロ','kg','kg',''],
        '数量単位（トン、箱、尾など）': [np.nan,np.nan,np.nan,np.nan,'トン',np.nan,np.nan],
        '価格単位（円/kg、円/箱など）': ['円/kg','円/箱','','円/キロ','円/トン','円','円/kg']
    }
    df_raw_test = pd.DataFrame(raw_data)
    df_processed_test = preprocess_market_data(df_raw_test.copy())
    print("\nテスト用処理済みデータ:")
    if not df_processed_test.empty: print(df_processed_test.head()); df_processed_test.info()