import pandas as pd
df = pd.read_excel('data/output/merged excels/abakkus/2026/CONSOLIDATED_ABAKKUS_2026_04.xlsx', sheet_name=1)
print(df.iloc[6:10].values.tolist())
