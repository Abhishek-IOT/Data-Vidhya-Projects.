import pandas as pd
import random



df=pd.read_csv('C:\\Users\\Dell\\Documents\\Data-Vidhya-Projects\\TasteTrack Analytics - Enterprise Restaurants Lakehouse\\Datasets\\restaurants_order.csv')
print(df.head())

waiter={
'John': 'W001',
'David': 'W002',
'Chris': 'W003',
'Ryan': 'W004',
'Alex': 'W005',
'Ethan': 'W006',
'Noah': 'W007',
'Liam': 'W008',
'Mason': 'W009',
'Lucas': 'W0010',
'Grace': 'W0011',
'Chloe': 'W0012',
'Ella': 'W0013',
'Lily': 'W0014',
'Ava': 'W0015',
'Mia': 'W0016',
'Zoe': 'W0017',
'Hannah': 'W0018',
'Nathan': 'W0019',
'Owen': 'W0020'
}
# print(waiter['John'])

# df['waiter']=[random.choice(waiter) for _ in range(len(df))]
# # print(df.head())


chef={
'Michael':'C001',
'Emma' :'C002',
'Daniel':'C003',
'Olivia':'C004',
'Sophia':'C005'
}

# df['chef']=[random.choice(chef) for _ in range(len(df))]
# print(df.head())

# df.to_csv('added.csv',index=False)
# print("Added")

# df['waiter'] = [i for i in wai]
for i in waiter.values():
    print(i)

df1=df.head()

for i in df1.itertuples():
    if i.waiter in waiter:
        df['waiter']=waiter[i.waiter]
    if i.chef in chef:
        df['chef']=chef[i.chef]



df.to_csv('added.csv',index=False)
print("Added")
