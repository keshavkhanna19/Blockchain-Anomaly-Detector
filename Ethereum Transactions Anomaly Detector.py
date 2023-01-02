#!/usr/bin/env python
# coding: utf-8

# # Data Collection Using Python Library Selenium and Etherscan API

# In[1]:


#Importing the necessary libraries to scrap the data 
import csv
from getpass import getpass
from time import sleep
from selenium.common.exceptions import NoSuchElementException
from selenium import webdriver
import pandas as pd
from selenium.webdriver.common.keys import Keys
import time
from selenium.webdriver.support.ui import Select
import numpy as np
from selenium.webdriver.common.keys import Keys


# In[2]:


PATH = '/Users/keshavkhanna/Downloads/chromedriver3'
driver = webdriver.Chrome(PATH)
driver.get("https://etherscan.io/accounts")
time.sleep(2)
driver.find_element_by_id("ContentPlaceHolder1_ddlRecordsPerPage").click()


# ## Now I will be scrapping latest 100 transactions of top 1000 accounts on Etherscan

# In[3]:


time.sleep(2)
#We will take all the transactions for 100 transactions per page
dropDown = Select(driver.find_element_by_id("ContentPlaceHolder1_ddlRecordsPerPage"))
dropDown.select_by_value('100')

Address_list = []

pages = 10

for page in range(pages):
    for address in driver.find_element_by_tag_name("table")    .find_element_by_tag_name("tbody").find_elements_by_tag_name("tr"):
        Address_list.append(address.find_element_by_tag_name("a").text)
        
    time.sleep(1)    
    driver.find_element_by_link_text('Next').click() 
    time.sleep(2)


# In[7]:


import pandas as pd
import requests

Big_df = pd.DataFrame()

for address in Address_list:
    
    time.sleep(5)
    url = "http://api.etherscan.io/api?module=account&action=txlist&address=" + address + "&startblock=0&endblock=99999999&page=1&offset=101&sort=desc&apikey=YourApiKeyToken"

    print(url)
    response = requests.get(url)

    address_content = response.json()
    result = address_content.get("result")
    
    print(result)

    

    for n,transaction in enumerate(result):
        hashe = transaction.get("hash")
        tx_from = transaction.get("from")
        tx_to = transaction.get("to")
        value = transaction.get("value")
        confirmations = transaction.get("confirmations")
        blockNumber = transaction.get("blockNumber")
        timeStamp = transaction.get("timeStamp")
        nonce = transaction.get("nonce")
        blockHash = transaction.get("blockHash")
        value = transaction.get("value")
        gas = transaction.get("gas")
        gasPrice = transaction.get("gasPrice")
        cumulativeGasUsed = transaction.get("cumulativeGasUsed")
        gasUsed = transaction.get("gasUsed")
        confirmations = transaction.get("confirmations")
        method = transaction.get("methodId")


        print("Time: ", timeStamp)
        print("Transaction ID: ", n)
        print("hash: ", hashe)
        print("from: ",tx_from)
        print("to: ", tx_to)
        print("value: ", value)
        print("Confirmations: ", confirmations)
        print("Block Number: ", blockNumber)
        print("Nonce: ",nonce)
        print("Block Hash: ", blockHash)
        print("Value: ", value)
        print("Gas: ", gas)
        print("Gas Price: ", gasPrice)
        print("Cumulative Gas Used: ", cumulativeGasUsed)
        print("Gas Used: ", gasUsed)
        print("Method: ", method)
        print("\n")


        Small_df = pd.DataFrame(pd.Series([timeStamp,hashe,tx_from,tx_to,value,confirmations,blockNumber,nonce,blockHash,value,gas
        ,gasPrice,cumulativeGasUsed,gasUsed,method],index=["Time","Hash","Transaction_From",
        "Transaction_To","Value","Confirmations","Block_Number","Nonce","Block_Hash","Value","Gas","Gas_Price"
                                                          ,"Cumulative_GasUsed","GasUsed","Method"])).transpose()



        Big_df = pd.concat([Small_df,Big_df])

    Big_df = Big_df.reset_index(drop=True)
    
    







# In[11]:


Big_df


# In[13]:


Big_df.to_csv("Ethereum_Dataset.csv")


# # Preprocessing the Data

# ##### Now we have got our dataset ready. Now begins the process of pre-processing 

# In[2]:


import pandas as pd 
ethData = pd.read_csv('Ethereum_Dataset.csv')
ethData


# ###### To check if it is a pandas DataFrame 

# In[3]:


type(ethData)


# In[4]:


#Our first step is to do finding the size, shape and dimensions of our dataset
size = ethData.size
shape = ethData.shape
ndim = ethData.ndim
print("Size of DataFrame: "+str(size)+",","Shape of DataFrame: "+ str(shape)+",","Dimensions of DataFrame: "+str(ndim)+",")


# In[5]:


#Now check the column names and delete duplicate columns
print(ethData.columns)

#We fill see only top 5 rows of our dataFrame 
ethData.head(5)


# In[6]:


#It looks like we can remove Unnamed since it is the same as index
ethData.drop('Unnamed: 0', axis=1, inplace=True)


# In[7]:


ethData


# In[8]:


#Now we will define a python function which returns us all the duplicate columns
def getDuplicateColumns(df):
    '''
    Get a list of duplicate columns.
    It will iterate over all the columns in dataframe and find the columns whose contents are duplicate.
    :param df: Dataframe object
    :return: List of columns whose contents are duplicates.
    '''
    duplicateColumnNames = set()
    for x in range(df.shape[1]):
        col = df.iloc[:, x]
        for y in range(x + 1, df.shape[1]):
            otherCol = df.iloc[:, y]
            if col.equals(otherCol):
                duplicateColumnNames.add(df.columns.values[y])
    return list(duplicateColumnNames)


# In[9]:


# Get list of duplicate columns
duplicateColumnNames = getDuplicateColumns(ethData)
print('Duplicate Columns are as follows')
for col in duplicateColumnNames:
    print('Column name : ', col)


# In[10]:


#Hence we will drop the column named Value.1
ethData = ethData.drop('Value.1', axis=1)


# In[11]:


ethData


# In[12]:


#We will also check for which columns we have N/A values such that we can do adjustments for them 
ethData.isna().sum()


# In[13]:


#For 9128 rows, we have only a few rows that give us N/A values such that we can just remove these rows
ethData = ethData.dropna(axis=0, inplace=False)
ethData.isna().sum()


# In[14]:


# convert time column which is in the form of timestamps to datetime object and datetime object to timestamp
from datetime import datetime
dateTimesList = []
for dateTimes in ethData['Time']:
    dateTimesList.append(datetime.fromtimestamp(dateTimes))


# In[15]:


#Now changing Time column to timeStamp values
ethData['Time']=dateTimesList


# In[16]:


ethData


# In[17]:


#We all see that the value column is multiplied 10^18. Hence we will create a new column named Value in Ether 
# which will be called valEther

ethData['valEther'] = ethData['Value'].astype('float')/10**18


# In[18]:


##We will create a new column named Value in Dollar 
#which will be called valDollar 

ethData['valDollar'] = ethData['valEther']*1635.62


# In[19]:


ethData['valDollar'] = ethData['valDollar'].astype('float')


# In[20]:


ethData


# In[21]:


#We will on extract those columns which we will use for our Machine Learning model
newEthData = ethData[['Hash','Time','valEther','valDollar']]

#We also need to sort our rows starting from the earliest time and reset the index of our DataFrame 
newEthData = newEthData.sort_values(by='Time').reset_index().drop('index',axis=1)


# In[22]:


newEthData


# # Feature Extraction and Machine Learning Technique Used 

# ##### To analyse malicious transactions, I will be using method of rolling window aggregation for time series. Each hash which is the trnasaction ID for each transaction has a timestamp of the transaction and the transaction value in its correponding USD value and Ether value at the time of the transaction
# 
# ##### The rolling window feature extraction for time series is a procedure, where time-series data are analyzed in sequence, from the earliest data in steps of size h, which could be defined as the number of measurements in the sequence or as the time frame
# 
# ##### The size of the rolling window is defined as w, which is the number of measurements in one window. The basic procedure of feature extraction for the last transaction, denoted by the gray color of the background, is shown in Figure 2.
# 
# ![C17B6C6E-0D24-4113-9F1C-76B576A13DD6.png](attachment:C17B6C6E-0D24-4113-9F1C-76B576A13DD6.png)
# 
# ##### Each of the time windows forms a new sequence, which is then used to calculate the aggregations of the sequential data. In this research, the size of the rolling window m was defined as the time frame and was not fixed to the number of individual transactions. In addition, the step size h was defined as one transaction. Since any blockchain address could have different patterns of transactions, and the goal of our research was to make a personalized anomalous transaction detection, several differenttime frames as windows sizes m were used: one second, one minute, one hour, one day, seven days, 14 days, 30 days, 60 days, and 90 days. Consequently, as the number of transactions varies across the windows, the size of the rolling window is not fixed. The goal of our research is not to determine which of these time frames is most relevant in general, but to build a procedure, which determines the appropriate time frame for each of the addresses individually.
# 
# ##### The aggregation functions f used on the transaction windows were the following statistical procedures: mean, median, standard deviation, sum, and count (number of transactions) of the transactions in the window. Aggregating the number of transactions enables the transformation of the time series data to tabular data (shown in Figure 2) and the usage of traditional anomaly detection methods. The combinations of time frames w and aggregation functions f are denoted as the combination wi fj, for each i ∈ [1, v] and j ∈ [1, k]. Value v is the number of different time frames used for windows sizes (in our experiment 9), and value k is the number of different aggregation functions used on the set of transactions in the windows (in our case 5). The extracted features for each transaction were thus the combinations wi fj of all of the time frames w and all of the functions f, resulting in 45 new extracted features in addition to the original USD value of the transaction.
# 
# 

# ### Hence to implement such methodology in Python code, we start by selecting only first 10 transactions and loop over first 10 then 9 then 8, and so on. 

# In[106]:


newEthData10 = newEthData.head(10)
for i in range(len(newEthData10)):
    print(newEthData10.loc[0:len(newEthData10)-i])


# ### Then we will calculate the aggregate functions (5) such as mean, median, standard deviation, sum, and count (number of transactions) over windows (9) of one second, one minute, one hour, one day, seven days, 14 days, 30 days, 60 days, and 90 days for our top 10 transactions. Such that we will have 5 * 9 = 45 additional features or columns for all our transactions. Therefore we will appply the same methodology to our original dataset ethData 

# In[102]:


newEthData10.rolling('2s').sum()


# ### Since after testing our rolling function, we see that our time aware rolling method (rolling for specific times) is not working (works in most of the Youtube videos I have seen). Once I get that to work and have completed my Feature Extraction process to obtain something as figure above, I will deploy my Machine Learning Technique on my Dataframe

# ### The anomaly detection method is used to construct the anomaly detection model. For the next j transaction, this model is used to evaluate each transaction, to be either normal or anomalous.
# 
# ### As our objective was to test the validity of the proposed method and not to find optimal settings, we limited ourselves to using only one unsupervised anomaly detection method—the Isolation Forest for novelty detection method by, which isolates each transaction and splits them into inliers (i.e., normal transaction) and outliers (i.e., anomalous transaction), based on the number of decisions in the decision tree to isolate the transaction. 
# 
# ### The nature of anomaly detection problems without labeled cases is such that there is no ground truth for the addresses. In other words, there is no ability to determine if the anomaly detection method has correctly labeled the transaction as anomalous or not. This is sometimes called novelty detection. Hence, we presented the results of unsupervised anomaly transaction detection in a time series chart, where detected anomalous transactions are clearly denoted.
# 
# ### This methodology was used in the article A Machine Learning-Based Method for Automated Blockchain Transaction Signing Including Personalized Anomaly Detection. They proposed anomalous transaction detection method on 10 Ethereum addresses. Other settings of the methods used in the experiment were the following. Isolation Forest consisted of 100 individual decision trees with the contamination factor of 0.01. The Random Forest classification algorithm was used to determine the feature importances and was also constructed out of 100 individual decision trees, built with Gini impurity split criteria. The experiment was implemented using the scikit-learn v0.21.3 Python package. All of the other settings were left at their default values.

# # Results

# ### As is normal for unsupervised anomaly detection, there is no precise way to determine if the transaction was a mistake, an error, or a fraudulent transaction, without contacting the address owners. Without the ground-truth labels, there is no clear way to evaluate the efficiency of the proposed method. For this reason, all of the results are presented in visual form for the readers to interpret and evaluate the proposed approach. The results of anomalous transaction detection are presented in a time series chart form. Anomalous transactions are denoted as red crosses, and normal transactions are denoted as blue dots. The first 100 transactions were only used as the starting training data, thus separating them with the dashed line.

# ### My method is very similar to the one proposed in the paper except that I would be analyzing ethereum transactions for close to 1000 top user addresses which was my aligns with my main purpose of scrapping latest 100 transactions of top 1000 accounts on Etherscan

# ![FF37324F-F45B-43D2-9DEF-7CA7EFC172D3.png](attachment:FF37324F-F45B-43D2-9DEF-7CA7EFC172D3.png)
