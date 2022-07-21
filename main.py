import os
import pandas as pd
import numpy as np
import sqlalchemy as sa
from flask import Flask,Response,jsonify
app = Flask(__name__)


def consolidated():
    '''
    consolidated dataframe and writes to an ouput consolidated_output_1.csv

    '''
    v, df2 = [], []
    for (dirpath, dirnames, filenames) in os.walk("Input"):
        for file in filenames:
            print(file)
            df = pd.read_csv(os.path.join(dirpath, file), sep=',|\|', engine='python')
            df["source_name"] = os.path.splitext(file)[0]
            df2.append(df)
    final_df = pd.concat(df2, axis=0, ignore_index=True)
    final_df.to_csv("output/consolidated_output_1.csv",index = False)

    #bonus 1
    #The `sample_data.1` file has a number of products we do not want in our output.
    #Filter this data so that the only products that remain are products with a `worth` of MORE than `1.00`
    final_df = final_df[(final_df["source_name"] == 'sample_data.1') &
                        (final_df['worth'] > 1)].append(final_df[final_df["source_name"] != 'sample_data.1']).reset_index(drop = True)


    final_df["worth"] = np.where(final_df["source_name"] == 'sample_data.3',final_df["worth"] * final_df["material_id"],final_df["worth"])

    first_df_group = final_df.groupby("product_name").agg({'quality' : 'first'})

    max_df_group = final_df.groupby(['product_name'], sort=False)['material_id'].max()

    sum_df_group = final_df.groupby(['product_name'], sort=False)['worth'].sum()

    material_df = pd.read_csv(r'Input/data_source_2/material_reference.csv')

    final_final_df = pd.merge(final_df, material_df, left_on='material_id', right_on='id')

    final_final_df.drop(final_final_df.filter(regex='_x$').columns, axis=1, inplace=True)

    final_final_df.columns = final_final_df.columns.str.replace('_y', '')

    # upload to sql server
    # we are using bulk insert here to upload data
    #engine = sa.create_engine('mssql+pyodbc://user:password@server/database',fast_executemany = True).connect()

    #final_final_df.to_sql(str, con=engine, index=False, if_exists='replace')

    return final_final_df




@app.route("/getdata" ,methods=["GET"])
def dfjson():
    """
    return a json representation of the dataframe
    """
    df = consolidated()

    result = {}
    for index, row in df.iterrows():
        # result[index] = row.to_json()
        result[index] = dict(row)
    return jsonify(result)




if __name__ == '__main__':
    app.run(debug=True)


