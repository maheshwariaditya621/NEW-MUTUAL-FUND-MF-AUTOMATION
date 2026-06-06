import pandas as pd
df = pd.read_excel('data/output/merged excels/abakkus/2026/CONSOLIDATED_ABAKKUS_2026_04.xlsx', sheet_name=1)
print(df.columns.tolist())
print(df.iloc[3:6].values.tolist())
