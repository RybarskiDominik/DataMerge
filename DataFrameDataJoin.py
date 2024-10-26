from dataclasses import dataclass, field
from typing import Optional, Tuple
import pandas as pd
import numpy as np
import logging
import re

logging.basicConfig(level=logging.INFO, filename="log.log", filemode="w", format="%(asctime)s - %(lineno)d - %(levelname)s - %(message)s") #INFO NOTSET


@dataclass
class myDataJoinFarame():

    df_main: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_main_original: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_main_header_status: bool = None
    df_main_base_col: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_main_base_col_name: str = None

    df_data: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_data_oryginal: pd.DataFrame = field(default_factory=pd.DataFrame)
    df_data_header_status: bool = None
    df_data_base_col_name: str = None
    df_status_merge: bool = True

    @classmethod
    def df_header(cls, df_name, header=False):
        df = None
        status = None
        if df_name == 'df_main' and header:
            if cls.df_main.empty:
                return False
            cls.df_main_original = cls.df_main.copy()
            df = cls.df_main
            status = cls.df_main_header_status = True
        elif df_name == 'df_data' and header:
            cls.df_data_oryginal = cls.df_data.copy()
            if cls.df_data_oryginal.empty:
                return False
            df = cls.df_data
            status = cls.df_data_header_status = True
        elif not header:
            pass
        else:
            return False
            #raise AttributeError(f"DataFrame '{df_name}' not found in class.")

        if header and status:
            df.columns = df.iloc[0]
            df = df[1:].reset_index(drop=True)
            setattr(cls, df_name, df)
        else:
            if df_name == "df_main"and not cls.df_main_original.empty:
                df = cls.df_main = cls.df_main_original.copy()
                cls.df_main_header_status = False
            elif df_name == "df_data" and not cls.df_data_oryginal.empty:
                df = cls.df_data = cls.df_data_oryginal.copy()
                cls.df_data_header_status = False
        return df

    @classmethod
    def main_base_merge_coll(cls, col):
        if col not in cls.df_main.columns:
            return False
            
        else:
            try:
                cls.df_main_base_col = cls.df_main[[col]]
            except Exception as e:
                logging.exception(e)
                print(e)

    @classmethod
    def display_data(cls):
        return cls.df_main

    @classmethod
    def is_duplicated(cls, 
                      df_name: Optional[str] = None, 
                      subset: str = "NR", 
                      df: Optional[pd.DataFrame] = None
                      ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        if df_name is None and df is None:
            raise ValueError("Either 'df_name' or 'df' must be provided.")
        if df is None or df.empty:
            df = getattr(cls, df_name)
        
        df_duplicates = df[df.duplicated(subset, keep='first')].copy()
        df_cleaned = df.drop_duplicates(subset)
        if df_name:
            setattr(cls, df_name, df_cleaned)

        return df_cleaned, df_duplicates

    @classmethod
    def initialize(cls):
        cls.df_main = pd.DataFrame(columns=['Main table where data will be merged.'])
        cls.df_main_original = pd.DataFrame()

        cls.df_data = pd.DataFrame(columns=['Table from which data will be joined.'])
        cls.df_data_original = pd.DataFrame()

    @classmethod
    def df_connexion(cls, df_status_merge=False):
        if df_status_merge:
            cls.df_status_merge = True
        df_merge = pd.DataFrame()
        if cls.df_main_base_col.empty or cls.df_data.empty:
            return False
        if cls.df_main_base_col_name not in cls.df_main_base_col.columns:
            return False
        if cls.df_data_base_col_name not in cls.df_data.columns:
            return False
        if cls.df_status_merge: # Stop jeżeli dokonano juz wcześniej zlączenia.
            try:
                df_merge = pd.merge(left=cls.df_main_base_col, right=cls.df_data, left_on=cls.df_main_base_col_name, right_on=cls.df_data_base_col_name, suffixes=["_drop","_drop"], how="left")
                df_merge = df_merge.drop(columns=[cls.df_main_base_col_name, cls.df_data_base_col_name, f'{cls.df_main_base_col_name}_drop', f'{cls.df_data_base_col_name}_drop'], errors='ignore')
                new_columns = [col for col in df_merge.columns if col in cls.df_main.columns]
                df_merge.rename(columns={col: f'New_{col}' for col in new_columns}, inplace=True)
                cls.df_main = pd.concat([cls.df_main, df_merge], axis = 1)
                cls.df_status_merge = False
            except Exception as e:
                logging.exception(e)
                print(e)
                return
        else:
            return ""
        return cls.df_main

    @classmethod 
    def clean(cls, df_name=None, df=None):  # Czyszczenie wierszy o nieodpowiednim formacie.
        if df is None or df.empty:
            df = getattr(cls, df_name)
        setattr(cls, df_name, df)
        return df

    @classmethod
    def sort(cls, df_name=None, name=None, df=None):  # Sortowanie danych.
        if df is None or df.empty:
            df = getattr(cls, df_name)
        df = df.copy()
        try:
            df['Rep'] = df[name].str.replace(r'[a-zA-Z]', '', regex=True)                
            df['SortN'] = df['Rep'].str.replace(r'[^a-zA-Z0-9]', '.', regex=True)
            df['SortL'] = df[name].str.replace(r'[^a-zA-Z]', '', regex=True)
            df.loc[df['SortN'] == '', 'SortN'] = np.nan    
            df['SortN'] = df['SortN'].astype(float)
            df = df.sort_values(['SortN', 'SortL'], na_position='first')
        except Exception as e:
            logging.exception(e)
            print(f'Sorting error {e}')
            return

        try:
            df.drop(columns=['Rep', 'SortN', 'SortL'], axis=1, inplace=True)
            df.reset_index(drop=True, inplace=True)
        except:
            pass

        setattr(cls, df_name, df)
        return df

    @classmethod
    def read(cls, df_name=None, path=None, names=None, header=None, separator=None):  # Czytanie danych z pliku.
        df = pd.DataFrame()
        try:
            if  path.endswith('.txt') or path.endswith('.csv'):
                df = pd.read_csv(path, sep = r'\s+', dtype=str, names=names, header = header, on_bad_lines = 'skip', skip_blank_lines = True)
            elif path.endswith('.xlsx'):
                df = pd.read_excel(path, dtype=str, names=names, header=header)
            else:
                return False
            if not header and not names:
                df.columns = [str(i+1) for i in range(df.shape[1])]
        except Exception as e:
            logging.exception(e)
            print(e)
            return False

        setattr(cls, df_name, df)
        setattr(cls, f"{df_name}_oryginal", df)
        return df

    @classmethod
    def read_manual(cls, df_name=None, path=None):
        data = []
        with open(path, mode='r') as file:
            for line in file:
                row = re.split(r"\s+", line.strip())
                data.append(row)
        df = pd.DataFrame(data)
        setattr(cls, df_name, df)
        return df


if __name__ == "__main__":
    myDataJoinFarame.initialize()
    path = "test.txt"
    path_join = "test_join.txt"

    myDataJoinFarame.read('df_main', path, header=None)
    myDataJoinFarame.read("df_data", path_join)
    myDataJoinFarame.df_header('df_main', True)
    myDataJoinFarame.df_header("df_data", True)

    myDataJoinFarame.df_main_base_col_name = "NR"
    myDataJoinFarame.df_data_base_col_name = "NR"

    myDataJoinFarame.main_base_merge_coll(myDataJoinFarame.df_main_base_col_name)

    myDataJoinFarame.df_connexion()

    #myDataJoinFarame.is_duplicated('df_main', "NR")
    #myDataJoinFarame.is_duplicated('df_data', "NR")

    print(myDataJoinFarame.df_main)