# %%
import pandas as pd
import numpy as np
import csv
from sklearn.utils import shuffle
from random import choice
from string import ascii_lowercase, digits
from sqlalchemy import create_engine, text as query_text
from sqlalchemy.engine.base import Engine
import glob
from uuid import uuid4


# %%
test_dataset_location = './test_dataset'
test_file_list = list(glob.glob('*/*.csv', recursive=True))


# %%
uid_master_table_cols = [
    'uid',
    'nid',
    'mid',
    'did',
    'lid'
]

# %%
username = 'pgadmin'
passwd = 'd60w554p'
sqlalchemy_dialect = f"""postgresql+psycopg2://{username}:{passwd}@localhost:5432/metrics"""
engine = create_engine(sqlalchemy_dialect)


# %%
uid_master_table = 'uid_master'


# %%
def gen_new_unique_id():
    return str(uuid4())


def read_data_from_table(engine: Engine, k, v,
                         table_name, where_clause=""):
    if not where_clause:
        if v != '':
            where_clause = f""" where "{k}" = '{v}' """
            query = f"""SELECT * FROM {table_name} {
                where_clause
            } LIMIT 1"""

            with engine.connect() as conn:
                try:
                    result = conn.execute(
                        query_text(
                            query
                        )
                    )
                    result_dict_list = result.mappings().all()
                except Exception as e:
                    if e.code == 'f405':
                        print("Table does not exist")
                        result_dict_list = []
                    else:
                        raise(e)
                if len(result_dict_list) == 1:
                    return result_dict_list[0]
                else:
                    return None
        else:
            return None
    else:
        query = f"""SELECT * FROM {table_name} {
                where_clause
            } LIMIT 1"""
        with engine.connect() as conn:
            try:
                result = conn.execute(
                    query_text(
                        query
                    )
                )
                result_dict_list = result.mappings().all()
            except Exception as e:
                if e.code == 'f405':
                    print("Table does not exist")
                    return None
                else:
                    raise(e)
            if len(result_dict_list) == 1:
                return result_dict_list[0]
            else:
                return None


# %%
def read_existing_data(engine: Engine, table_name=uid_master_table, column_params={}):
    data = None
    for k, v in column_params.items():
        data = read_data_from_table(engine, k, v, table_name=table_name)
        if data:
            return data
    if all(map(lambda x: x=='',list(column_params.values()))):
        where_clause = """ where ( "nid" ='' and "mid"='' and "did" = '' and "lid"='' ) """
        data = read_data_from_table(engine, k='', v='',
                                   table_name=table_name, 
                                   where_clause=where_clause)
        return data

    # print(query)


# %%
def insert_records_columns(engine: Engine,
                           table_name,
                           column_params):
    column_list = list(column_params.keys())
    columns = '" , "'.join(column_list)
    if columns:
        columns = f'"{columns}"'
    data_list = list(column_params.values())
    data = "' , '".join(data_list)
    if data:
        data = f"""'{data}'"""
    query = f"""INSERT INTO {
        table_name
    } ({
        columns
    }) VALUES ({
        data
    })"""
    try:
        with engine.connect() as conn:
            result = conn.execute(
                query_text(
                    query
                )
            )
    except Exception as e:
        if e.code == 'f405':
            print("Table does not exist")
            column_list = []
            for _ in uid_master_table_cols:
                column_list.append(f'"{_}"')
            column_form = ""
            new_column_list = []
            for col in column_list:
                if col == '''"uid"''':
                    new_column_list.append(
                        f''' {col} text PRIMARY KEY '''
                    )
                else:
                    new_column_list.append(f""" {col} text """)
            column_form_str = ', '.join(
                new_column_list
            )
            create_table_query = f"""CREATE TABLE IF NOT EXISTS {table_name} ( {column_form_str} );"""
            with engine.connect() as conn:
                result = conn.execute(
                    query_text(
                        create_table_query
                    )
                )
        else:
            raise(e)
        print(e)


# %%
def update_records_column(engine: Engine,
                          table_name,
                          updatable_value,
                          conditional_values):
    update_list = []
    for k, v in updatable_value.items():
        update_list.append(f""" "{k}"='{v}' """)

    update = " , ".join(update_list)

    conditions_list = []
    for k, v in conditional_values.items():
        conditions_list.append(f""" "{k}"='{v}' """)
    condition = " and ".join(conditions_list)

    query = f"""UPDATE {
        table_name
    } SET {
        update
    } where {condition} """
    with engine.connect() as conn:
        result = conn.execute(
            query_text(
                query
            )
        )


# %%
def upsert_records(engine: Engine, table_name, column_params):
    # print(column_params)
    existing_data = read_existing_data(engine, table_name, column_params)
    if existing_data:
        updatable_values = {}
        conditional_values = {
            'uid': existing_data['uid']
        }
        for k, v in column_params.items():
            if v != '' and existing_data[k] == '':
                updatable_values[k] = v
            elif v != '' and existing_data[k] != '':
                conditional_values[k] = v
        if not all(
            map(lambda x: x != '', list(updatable_values.values()))
        ):
            update_records_column(
                engine,
                table_name,
                updatable_values,
                conditional_values
            )
    else:
        column_params['uid'] = gen_new_unique_id()
        insert_records_columns(engine,
                               table_name,
                               column_params
                               )


# %%
def create_database(file: str, engine: Engine = engine,
                    table_name=uid_master_table):
    def fn_uuid_table(x):
        return upsert_records(
            engine=engine,
            table_name=table_name,
            column_params=x.to_dict(),
        )
    df = pd.read_csv(file)
    param_cols = []
    for _ in uid_master_table_cols:
        if _ in df.columns:
            param_cols.append(_)
    df_x = df.copy()[param_cols]
    for _ in df_x.columns:
        df_x.loc[df_x.index, _] = df_x[_].fillna("")
    df_x.apply(fn_uuid_table, axis=1)


# %%
create_database(test_file_list[0])


# %% [markdown]
# d1 = {
#     'nid': 'lufi62kyrsdg',
#     'mid': 'xvddexr42mv4',
#     'did': '',
#     'lid': '',
# }
# read_existing_data(engine, table_name='test', column_params=d1)
#

# %% [markdown]
# df = pd.read_csv('./data_created.csv')
#

# %% [markdown]
# for _ in df.columns:
#     df.loc[df.index, _] = df[_].fillna('')
#

# %% [markdown]
# df.to_sql('test', engine, if_exists='replace', index=False)
#

# %% [markdown]
# from datetime import datetime
#

# %% [markdown]
# df.loc[df.index, 'date'] = [
#     datetime.strptime(
#         f"""{
#         np.random.randint(11,28)
#     }-{
#         np.random.randint(1,12)
#     }-{
#         np.random.randint(2017,2020)
#     }""",
#         "%d-%m-%Y"
#     ) for _ in range(df.shape[0])
# ]
#

# %% [markdown]
# df.groupby(['date']).count()
#

# %% [markdown]
# c = '" and "'.join([str(1), str(2)])
#

# %% [markdown]
# c = '" and "'.join([])
#

# %% [markdown]
# if c:
#     print(f'"{c}"')
# else:
#     print(".")
#

# %%
