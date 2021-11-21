# %%
import pandas as pd
import numpy as np
import csv
from sklearn.utils import shuffle
from random import choice
from string import ascii_lowercase, digits
from datetime import datetime


# %%
def generate_testdata():

    chars = ascii_lowercase + digits
    df_old = pd.read_csv('./data.csv')
    df = pd.DataFrame(
        [
            [
                ''.join(choice(chars) for _ in range(12)) for _ in range(4)
            ]
            for _ in range(1000)
        ], columns=df_old.columns
    )

    df_list = []
    for _ in range(100):
        dfz = df.copy()
        for _ in dfz.columns:
            a = np.random.randint(1, 500)
            b = np.random.randint(a, 1000)
            dfz.loc[a:b, _] = None
            dfz.loc[:, _].apply(str)
        df_list.append(dfz)
        for _ in df.columns:
            df.loc[:, _].apply(str)
    df_list.append(df)
    final_df = pd.concat(df_list, ignore_index=True)
    final_df.loc[final_df.index, 'date'] = [
        datetime.strptime(
            f"""{
        np.random.randint(11,28)
    }-{
        np.random.randint(1,12)
    }-{
        np.random.randint(2017,2020)
    }""",
            "%d-%m-%Y"
        ) for _ in final_df.index]
    return final_df


# %%
final_df = generate_testdata()


# %%
shuffled_final_df = shuffle(final_df).reset_index(drop=True)


# %%
def generate_csv_shuffled(shuffled_final_df):
    a = 0
    b = shuffled_final_df.shape[0]-1000
    i=1
    while a<(b-2000):
        if i==1:
            a = a+1000
        x = np.random.randint(a, b)
        _df = shuffled_final_df.iloc[a:x]
        _df.to_csv(
            f'./test_datasets/data_shuffle_created_{i}.csv', 
            index=False,
        )
        i = i+1
        a = x
    _df = shuffled_final_df.iloc[a:b]
    _df.to_csv(
        f'./test_datasets/data_shuffle_created_{i}.csv',
        index=False,
    )


# %%
if __name__=='__main__':
    generate_csv_shuffled(shuffled_final_df)

# %% [markdown]
# dialect = f"""postgresql+psycopg2://pgadmin:d60w554p@localhost:5432/metrics"""
# 

# %% [markdown]
# engine = create_engine(dialect)

# %% [markdown]
# final_df.to_sql(
#     'test',
#     engine,
#     if_exists='append', 
#     index=False
# )

# %% [markdown]
# shuffle(df).reset_index(drop=True)

# %% [markdown]
# shuffled_final_df.to_csv(
#     './data_shuffle_created.csv', index=False
# )
# 

# %%



