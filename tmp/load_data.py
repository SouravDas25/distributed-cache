import pandas as pd

import numpy as np


def main():
    # df = pd.ExcelFile('all_roles.xlsx')
    # print(df.sheet_names)
    # print()

    df = pd.read_csv('all_roles.csv')

    print(df.head())
    # df.to_csv("all_roles.csv", index=False)

    filtered_df = df[df['Role'].str.contains('ZSG', case=False, regex=False)]

    filtered_df.to_csv('filtered_roles.csv', index=False)

    pass


if __name__ == '__main__':
    main()
