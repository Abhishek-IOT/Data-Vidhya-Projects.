import pandas as pd
import random



df=pd.read_csv('C:\\Users\\Dell\\Documents\\Data-Vidhya-Projects\\TasteTrack Analytics - Enterprise Restaurants Lakehouse\\Datasets\\restaurants_order.csv')
print(df.head())

waiter=[
'John',
'David',
'Chris',
'Ryan',
'Alex',
'Ethan',
'Noah',
'Liam',
'Mason',
'Lucas',
'Grace',
'Chloe',
'Ella',
'Lily',
'Ava',
'Mia',
'Zoe',
'Hannah',
'Nathan',
'Owen'
]

df['waiter']=[random.choice(waiter) for _ in range(len(df))]
# print(df.head())


chef=[
'Michael',
'Emma',
'Daniel',
'Olivia',
'Sophia'
]

df['chef']=[random.choice(chef) for _ in range(len(df))]
print(df.head())

df.to_csv('added.csv',index=False)
print("Added")

