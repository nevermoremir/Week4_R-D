import pandas as pd
from pymongo import MongoClient
import pymongo
import numpy as np

client = MongoClient('localhost', 27017) # MongodDb connection
db = client['tibit_pon_controller'] # db name

col = db['STATS-OLT-70b3d55236da'] # collection name
col2 = db['STATS-ONU-ALPHe3a69d67'] # collection 2
col3 = db['STATS-ONU-ALPHe3a69d94']# collection 3
       #     value_test2.append(c3oltpon0tx[b])

res = col.find() # get all record
res2 =col2.find() # get all record
res3 = col3.find() #get all record
dates = [] # create empty dates array
dates2 = [] # empty array for dates in collection two
dates3 = []# empty array for dates in collection three
for r in res: #loop all record from mongo
    dates.append(r['_id'].split(" ")[0]) # append each date only take the date

for r in res2: #loop all record from mongo
    dates2.append(r['_id'].split(" ")[0]) # append each date only take the date
for r in res3: #loop all record from mongo
    dates3.append(r['_id'].split(" ")[0]) # append each date only take the date

df = pd.DataFrame() # create empty dataframe
df2 = pd.DataFrame() # create empty dataframe
df3 = pd.DataFrame() # create empty dataframe
df['dates'] = dates #assign dates added at line 14 to dataframe create above
df2['dates'] = dates2 #assign dates added at line 14 to dataframe create above
df3['dates'] = dates3 #assign dates added at line 14 to dataframe create above

dates = df['dates'].unique()
dates2 = df2['dates'].unique()
dates3 = df3['dates'].unique()

sum=0
sumtx=0
sumponrx=0
sumpontx =0

d1 = [] #create date1 empty array
d2 = [] # temporary array for sorting date C2
d3 = [] # temporary array for sorting data c3

sumoltnni = []
sumoltnnitx = []
sumoltpon = []
sumoltpontx =[]
value_test =[]
value_test1=[]
value_test2 =[]
value_test3=[]
check = pd.DataFrame()
count=1

#Collection one retrieve data
for d in dates: #loop over all unique date from line 18
    #print(d) #print the current date running for the list
    #print(count)
    res = list(col.find({'_id':{'$regex': str(d)}}).sort([('_id', pymongo.DESCENDING)])) #get the latest data for the date
   # print("lenght for" ,'d', len(res))
    oltnni = []
    oltpon = []
    oltnnitx=[]
    oltpontx = []

    for b in range(len(res)):
        df = pd.DataFrame(res[b]) # create dataframe for response
        df.reset_index(inplace=True) # add number index to dataframe
        oltnni.append(df[df['index']=='RX Frames Green']['OLT-NNI'].iloc[0]) # filter for speicific index and add to array
        oltpon.append(df[df['index']=='RX Frames Green']['OLT-PON'].iloc[0]) # same as above
        sum=sum+oltnni[b]
        sumponrx+=oltpon[b]
        #if (count == 59):
         #   value_test.append(oltnni[b])


        oltnnitx.append(df[df['index']=='TX Frames Green']['OLT-NNI'].iloc[0]) # filter for speicific index and add to array
        oltpontx.append(df[df['index']=='TX Frames Green']['OLT-PON'].iloc[0]) # same as above
        sumtx+=oltnnitx[b]
        sumpontx+=oltpontx[b]
        #if (count == 1):
         #   value_test2.append(oltpon[b])
        #if (count == 59):
            #value_test1.append(oltpontx[b])
           # value_test1.append(oltnnitx[b])

    sumoltnni.append(sum)
    sumoltnnitx.append(sumtx)
    sumoltpon.append(sumponrx)
    sumoltpontx.append(sumpontx)

    sum=0
    sumtx=0
    sumponrx=0
    sumpontx =0
    d1.append(d) # append date into empty date array from line 20
    count +=1

print('count=',count)
#Variable temporary for collection two
sumc2rx=0
sumc2tx=0
sumc2omcctx=0
sumc2omccrx=0
sum_all_c2rx=[]
sum_all_c2tx=[]
sum_all_omcc_c2tx=[]
sum_all_omcc_c2rx=[]
count=1
df_t=pd.DataFrame()
count_new=0
#Collection two retrieve data
for d in dates2: #loop over all unique date from line 18
    print('c2',d) #print the current date running for the list

    res2 = list(col2.find({'_id':{'$regex': str(d)}}).sort([('_id', pymongo.DESCENDING)])) #get the latest data for the date
    print("lenght for" ,'d', len(res2))
    c2oltpon0tx=[]
    c2oltpon0rx=[]
    c2oltomcctx=[]
    c2oltomccrx=[]
    for b in range(len(res2)):
        df = pd.DataFrame(res2[b]) # create dataframe for response
        df.reset_index(inplace=True) # add number index to dataframe
        #if count ==59:
         #   count_new+=1
        #if count_new==2:
        #    df_t=df

        if 'OLT-PON0' in df:
            if df.isin(['RX Frames Green']).any().any():
                #print("ada")
                c2oltpon0tx.append(df[df['index']=='TX Frames Green']['OLT-PON0'].iloc[0]) # filter for speicific index and add to array
                c2oltpon0rx.append(df[df['index']=='RX Frames Green']['OLT-PON0'].iloc[0]) # filter for speicific index and add to array
            else:
               # print('xde')
                c2oltpon0rx.append(0)
                c2oltpon0tx.append(0)
        elif 'OLT-PON Service 0' in df:
            if df.isin(['RX Frames Green']).any().any():
                #print("ada je")
                c2oltpon0tx.append(df[df['index']=='TX Frames Green']['OLT-PON Service 0'].iloc[0]) # filter for speicific index and add to array
                c2oltpon0rx.append(df[df['index']=='RX Frames Green']['OLT-PON Service 0'].iloc[0]) # filter for speicific index and add to array

            else:
                c2oltpon0rx.append(0)
                c2oltpon0tx.append(0)
        else:
            #print("tak jumpe oltpon0&s0")
            c2oltpon0rx.append(0)
            c2oltpon0tx.append(0)
        if 'OLT-OMCC' in df:
            if df.isin(['RX Frames Green']).any().any():
                #print('omcc ade')
                c2oltomccrx.append(df[df['index']=='RX Frames Green']['OLT-OMCC'].iloc[0]) # filter for speicific index and add to array
                c2oltomcctx.append(df[df['index']=='TX Frames Green']['OLT-OMCC'].iloc[0]) # filter for speicific index and add to array
            else:
                c2oltomccrx.append(0)
                c2oltomcctx.append(0)

        else:
            c2oltomccrx.append(0)
            c2oltomcctx.append(0)

        #if count == 2:
         #   value_test.append(c2oltpon0rx[b])
          #  value_test1.append(c2oltomcctx[b])

        sumc2rx+=c2oltpon0rx[b]
        sumc2tx+=c2oltpon0tx[b]
        sumc2omccrx+=c2oltomccrx[b]
        sumc2omcctx+=c2oltomcctx[b]
        #if (count == 17):
         #   value_test.append(c2oltpon0rx[b])
        #if (count == 17):
         #   value_test2.append(c2oltpon0tx[b])


    sum_all_c2rx.append(sumc2rx)
    sum_all_c2tx.append(sumc2tx)
    sum_all_omcc_c2rx.append(sumc2omccrx)
    sum_all_omcc_c2tx.append(sumc2omcctx)
    sumc2tx=0
    sumc2rx=0
    sumc2omccrx=0
    sumc2omcctx=0

    d2.append(d) # append date into empty date array from line 20
    count +=1


#print(len(d2))
#print(count_new)

#Variable temporary for collection three
sumc3rx=0
sumc3tx=0
sumc3omccrx=0
sumc3omcctx=0
sum_all_c3rx=[]
sum_all_c3tx=[]
sum_all_omcc_c3rx=[]
sum_all_omcc_c3tx=[]
count=1
#Collection three retrieve data
for d in dates3: #loop over all unique date from line 18
    print('c3',d) #print the current date running for the list

    res3 = list(col3.find({'_id':{'$regex': str(d)}}).sort([('_id', pymongo.DESCENDING)])) #get the latest data for the date
    print("lenght for" ,'d', len(res3))
    c3oltpon0tx=[]
    c3oltpon0rx=[]
    c3oltomccrx=[]
    c3oltomcctx=[]
    for b in range(len(res3)):
        df = pd.DataFrame(res3[b]) # create dataframe for response
        df.reset_index(inplace=True) # add number index to dataframe
        if 'OLT-PON0' in df:
            if df.isin(['RX Frames Green']).any().any():
                #print("ada")
                c3oltpon0tx.append(df[df['index']=='TX Frames Green']['OLT-PON0'].iloc[0]) # filter for speicific index and add to array
                c3oltpon0rx.append(df[df['index']=='RX Frames Green']['OLT-PON0'].iloc[0]) # filter for speicific index and add to array
            else:
               # print('xde')
                c3oltpon0rx.append(0)
                c3oltpon0tx.append(0)
        elif 'OLT-PON Service 0' in df:
            if df.isin(['RX Frames Green']).any().any():
                #print("ada je")
                c3oltpon0tx.append(df[df['index']=='TX Frames Green']['OLT-PON Service 0'].iloc[0]) # filter for speicific index and add to array
                c3oltpon0rx.append(df[df['index']=='RX Frames Green']['OLT-PON Service 0'].iloc[0]) # filter for speicific index and add to array

            else:
                c3oltpon0rx.append(0)
                c3oltpon0tx.append(0)
        else:
            #print("tak jumpe oltpon0&s0")
            c3oltpon0rx.append(0)
            c3oltpon0tx.append(0)
        if 'OLT-OMCC' in df:
            if df.isin(['RX Frames Green']).any().any():
                #print('omcc ade')
                c3oltomccrx.append(df[df['index']=='RX Frames Green']['OLT-OMCC'].iloc[0]) # filter for speicific index and add to array
                c3oltomcctx.append(df[df['index']=='TX Frames Green']['OLT-OMCC'].iloc[0]) # filter for speicific index and add to array
            else:
                c3oltomccrx.append(0)
                c3oltomcctx.append(0)

        else:
            c3oltomccrx.append(0)
            c3oltomcctx.append(0)

        #if count == 59:
         #   value_test.append(c3oltpon0rx[b])
          #  value_test1.append(c3oltomcctx[b])


        sumc3rx+=c3oltpon0rx[b]
        sumc3tx+=c3oltpon0tx[b]
        sumc3omccrx+=c3oltomccrx[b]
        sumc3omcctx+=c3oltomcctx[b]




    sum_all_c3rx.append(sumc3rx)       #     value_test2.append(c3oltpon0tx[b])
    sum_all_c3tx.append(sumc3tx)
    sum_all_omcc_c3rx.append(sumc3omccrx)
    sum_all_omcc_c3tx.append(sumc3omcctx)

    sumc3tx=0
    sumc3rx=0
    sumc3omccrx=0
    sumc3omcctx=0
    d3.append(d) # append date into empty date array from line 20
    count +=1









#dftry =pd.DataFrame()
#dftry['tx&rx']= oltnni
#dftry.to_excel('test5newarray.xlsx',index = False)

dsum = pd.DataFrame()
dsum['dates'] = d1
dsum['total rx for oltnni'] =sumoltnni
dsum['totoal tx for oltnni'] =sumoltnnitx
dsum['total rx for oltpon'] = sumoltpon
dsum['total tx for oltpon'] = sumoltpontx
dsum.to_excel('sum_c1_newdata.xlsx',index = False)

dsumc2= pd.DataFrame()
dsumc2['dates'] = d2
dsumc2['total rx for oltpon0']= sum_all_c2rx
dsumc2['total tx for oltpon0'] =sum_all_c2tx
dsumc2['total rx for oltomcc'] = sum_all_omcc_c2rx
dsumc2['total tx for oltomcc'] = sum_all_omcc_c2tx
dsumc2.to_excel('Sum_c2_newdata.xlsx', index=False)

dsumc3= pd.DataFrame()
dsumc3['dates'] = d3
dsumc3['total rx for oltpon0']= sum_all_c3rx
dsumc3['total tx for oltpon0'] =sum_all_c3tx
dsumc3['total rx for oltomcc'] = sum_all_omcc_c3rx
dsumc3['total tx for oltomcc'] = sum_all_omcc_c3tx
dsumc3.to_excel('Sum_c3_newdata.xlsx', index=False)


#d_date=pd.DataFrame (d2)
#d_date.to_excel('datec2.xlsx', index = False)

#dvalue = pd.DataFrame(value_test)
#dvalue.to_excel('data_c3_pon0rx_1_9.xlsx', index=False)


#dvalue1 = pd.DataFrame(value_test1)
#dvalue1.to_excel('data_c3_omcctx_1_9.xlsx', index=False)

"""
dvalue2 = pd.DataFrame(value_test2)
dvalue2.to_excel('data_c1_oltponrx_6_8_2020.xlsx', index=False)

dvalue3 = pd.DataFrame(value_test3)
dvalue3.to_excel('data_c1_oltpontx_6_8_2020.xlsx', index=False)
"""


#df_t.to_excel('datatestingcheck2.xlsx', index= False)
