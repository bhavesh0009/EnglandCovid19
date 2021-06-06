import pandas as pd
#from uk_covid19 import Cov19API
import requests
import json
from datetime import timedelta, datetime
import numpy as np
import requests
import time
from time import sleep

def getCasesData():
    '''
    request = requests.get(
        "https://coronavirus.data.gov.uk/downloads/json/coronavirus-cases_latest.json")
    requestJson = json.loads(request.content)
    ltlasDf = pd.DataFrame(requestJson['ltlas'])
    lastRefresh = pd.DataFrame(requestJson['metadata'], index=[0])
    '''
    """ ltla_filter = ['areaType=ltla']
    cases_and_deaths = {
                        "areaType":"areaType"
                        ,"areaName":"areaName"
                        ,"areaCode":"areaCode"
                        ,"specimenDate":"date"
                        ,"dailyLabConfirmedCases":"newCasesBySpecimenDate"
                        ,"totalLabConfirmedCases":"cumCasesBySpecimenDate"
                        ,"cumDeaths28DaysByDeathDate":"cumDeaths28DaysByDeathDate"}
    api = Cov19API(filters=ltla_filter, structure=cases_and_deaths)
    data = api.get_json()  # Returns a dictionary """

    '''
    ltlasDf = pd.read_csv("https://c19downloads.azureedge.net/downloads/csv/coronavirus-cases_latest.csv")
    ltlasDf = ltlasDf.rename(columns=
        {'Area type':'areaType'
        ,"Area name":"areaName"
        ,"Area code":"areaCode"
        ,"Specimen date":"specimenDate"
        ,"Daily lab-confirmed cases":"dailyLabConfirmedCases"
        ,"Cumulative lab-confirmed cases":"totalLabConfirmedCases"
        })
    ltlasDf = ltlasDf[['areaType','areaName','areaCode','specimenDate','dailyLabConfirmedCases','totalLabConfirmedCases']]                        
    now = datetime.now()
    lastUpdate = now.strftime("%d/%m/%Y %H:%M:%S")
    '''
    lastUpdate = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    lastRefresh = [{"lastUpdatedAt": lastUpdate}]
    lastRefresh = pd.DataFrame(lastRefresh)
    #ltlasDf = pd.DataFrame(data['data'])
    ltlasDf = pd.read_csv("https://api.coronavirus.data.gov.uk/v2/data?areaType=ltla&metric=cumCasesBySpecimenDate&metric=newCasesBySpecimenDate&metric=cumDeaths28DaysByDeathDate&format=csv")
    cases_and_deaths = {
                    "areaType":"areaType"
                    ,"areaName":"areaName"
                    ,"areaCode":"areaCode"
                    ,"date":"specimenDate"
                    ,"newCasesBySpecimenDate":"dailyLabConfirmedCases"
                    ,"cumCasesBySpecimenDate":"totalLabConfirmedCases"
                    ,"cumDeaths28DaysByDeathDate":"cumDeaths28DaysByDeathDate"}

    ltlasDf = ltlasDf.rename(cases_and_deaths, axis='columns')                    
    return ltlasDf, lastRefresh

def getMSOAData():
    d = requests.get("https://api.coronavirus.data.gov.uk/v2/data?areaType=msoa&metric=newCasesBySpecimenDateRollingSum&format=json").json()
    mosa_df = pd.DataFrame.from_dict(d['body'])
    mosa_df['date'] = pd.to_datetime(mosa_df['date'])
    # # mosa_df = mosa_df[mosa_df.date == mosa_df.date.max()]
    # # mosa_df = mosa_df[['ltlacode','ltlaname','newcasesbyspecimendaterollingsum','areacode','areaname']]
    # # mosa_df = mosa_df.rename(columns={"ltlacode": "lad19_cd", 
    # #                         "ltlaname": "lad19_nm",
    # #                         "newcasesbyspecimendaterollingsum": "latest_7_days",
    # #                         "areacode": "msoa11_cd",
    # #                         "areaname": "msoa11_hclnm"})
    # # mosa_df['latest_7_days'] = mosa_df['latest_7_days'].fillna(0).astype('int')          
    all_msoa = mosa_df[['LtlaCode','LtlaName','areaCode','areaName']].drop_duplicates()
    mosa_df = mosa_df[mosa_df.date == mosa_df.date.max()]
    mosa_df = all_msoa.merge(mosa_df,
    how='left',
    left_on='areaCode',
    right_on='areaCode')[['LtlaCode_x','LtlaName_x','newCasesBySpecimenDateRollingSum','areaCode','areaName_x']]
    mosa_df = mosa_df.rename(columns={"LtlaCode_x": "lad19_cd", 
                            "LtlaName_x": "lad19_nm",
                            "newCasesBySpecimenDateRollingSum": "latest_7_days",
                            "areaCode": "msoa11_cd",
                            "areaName_x": "msoa11_hclnm"})
    mosa_df['latest_7_days'] = mosa_df['latest_7_days'].fillna(0).astype('int')    
         
    dfCoords = pd.read_csv(r"C:\Users\Projects\Documents\Engliand Covid Data\data\msoa_coords.csv")
    mosa_df = mosa_df.merge(dfCoords, how="left", left_on="msoa11_cd", right_on="msoaCD")
    #mosa_df = dfCoords.merge(mosa_df, how="left", left_on="msoaCD", right_on="msoa11_cd")
    #mosa_df['latest_7_days'] = mosa_df['latest_7_days'].fillna(0).astype('int')
    mosa_df = mosa_df[['lad19_cd', 'lad19_nm', 'latest_7_days', 'msoa11_cd', 'msoa11_hclnm', 'msoaLong', 'msoaLat','population']]
    mosa_df['latest_7_days'] = mosa_df['latest_7_days'].fillna(0)
    return mosa_df

def getGeoData():
    population = pd.read_csv("data/population.csv")
    lowerToUpperDf = pd.read_csv(
    "data/Lower_Tier_Local_Authority_to_Upper_Tier_Local_Authority.csv")
    return population, lowerToUpperDf


def processData(ltlasDf, population, lowerToUpperDf):
    ltlasDf.specimenDate = pd.to_datetime(ltlasDf.specimenDate)

    ltlasDf = ltlasDf[['areaCode', 'areaName', 'specimenDate',
                       'dailyLabConfirmedCases', 'totalLabConfirmedCases', 'cumDeaths28DaysByDeathDate']]
    #ltlasDf = ltlasDf[~(ltlasDf.areaCode.str.startswith('S'))]
    #ltlasDf = ltlasDf[~(ltlasDf.areaCode.str.startswith('N'))]

    df1 = ltlasDf.drop_duplicates(subset=['areaCode', 'areaName'])[
        ['areaCode', 'areaName']]
    dtDf = pd.DataFrame(pd.date_range(ltlasDf.specimenDate.min(
    ), ltlasDf.specimenDate.max(), freq='1 D'), columns=['specimenDate'])

    df1['key'] = 0
    dtDf['key'] = 0

    df1 = df1.merge(dtDf, how='outer').drop(columns=['key'], axis=1)
    ltlasDf = df1.merge(ltlasDf, how='left', on=[
                        'areaCode', 'areaName', 'specimenDate']).fillna(0)


    ltlasDf.dailyLabConfirmedCases = ltlasDf.dailyLabConfirmedCases.fillna(0)
    ltlasDf.cumDeaths28DaysByDeathDate = ltlasDf.cumDeaths28DaysByDeathDate.fillna(0) 

    ltlasDf['areaMovingAverage7'] = ltlasDf.groupby(
        'areaCode')['dailyLabConfirmedCases'].transform(lambda x: x.rolling(7, 1).mean())

    population.Area = population.Area.str.replace(',', '').astype('int')
    population.Population = population.Population.str.replace(
        ',', '').astype('int')
    ltlasDf = pd.merge(
        left=ltlasDf,
        right=population,
        how="left",
        left_on="areaCode",
        right_on="Code")
    ltlasDf = ltlasDf.drop(['Code', 'Name'], axis=1)
    ltlasDf['rate'] = (ltlasDf['dailyLabConfirmedCases'] /
                       ltlasDf.Population) * 100000


    ltlasAllSumDf = ltlasDf.groupby(['areaCode', 'areaName']).agg({'dailyLabConfirmedCases': 'sum',
                                                                'cumDeaths28DaysByDeathDate': 'max',
                                                                'rate': 'sum',
                                                                'Population': 'max',
                                                                'Area': 'max'}).reset_index()
    # Adding Last 30 days cases and growth
    tmp = ltlasDf[ltlasDf.specimenDate > (ltlasDf.specimenDate.max() - timedelta(days=30))].groupby('areaCode')['dailyLabConfirmedCases'].sum()
    ltlasAllSumDf['last30dCases'] = ltlasAllSumDf['areaCode'].map(tmp)
    tmp = ltlasDf[ltlasDf.specimenDate > (ltlasDf.specimenDate.max() - timedelta(days=30))].groupby('areaCode')['rate'].sum()
    ltlasAllSumDf['last30dRate'] = ltlasAllSumDf['areaCode'].map(tmp)    

    # Adding Last 7 days cases and growth
    tmp = ltlasDf[ltlasDf.specimenDate > (ltlasDf.specimenDate.max() - timedelta(days=7))].groupby('areaCode')['dailyLabConfirmedCases'].sum()
    ltlasAllSumDf['last7dCases'] = ltlasAllSumDf['areaCode'].map(tmp)
    tmp = ltlasDf[ltlasDf.specimenDate > (ltlasDf.specimenDate.max() - timedelta(days=7))].groupby('areaCode')['rate'].sum()
    ltlasAllSumDf['last7dRate'] = ltlasAllSumDf['areaCode'].map(tmp)

    tmp = ltlasDf[(ltlasDf.specimenDate >= (ltlasDf.specimenDate.max() - timedelta(days=17))) & (ltlasDf.specimenDate <= (ltlasDf.specimenDate.max() - timedelta(days=4)))].groupby('areaCode')['rate'].sum()
    tmp1 = ltlasDf[(ltlasDf.specimenDate >= (ltlasDf.specimenDate.max() - timedelta(days=31))) & (ltlasDf.specimenDate <= (ltlasDf.specimenDate.max() - timedelta(days=18)))].groupby('areaCode')['rate'].sum()
    ltlasAllSumDf['rFirst14'] = ltlasAllSumDf['areaCode'].map(tmp)
    ltlasAllSumDf['rSecond14'] = ltlasAllSumDf['areaCode'].map(tmp1)
    ltlasAllSumDf['rBasic'] = np.round(ltlasAllSumDf['rFirst14'] /  ltlasAllSumDf['rSecond14'],2)
    ltlasAllSumDf['rBasic'] = ltlasAllSumDf['rBasic'].fillna(0)
    #ltlasAllSumDf['rBasic'] = ltlasAllSumDf['rBasic'].replace(np.inf, ltlasAllSumDf['rFirst14'])
    ltlasAllSumDf.loc[ltlasAllSumDf.rBasic == np.inf, 'rBasic'] = ltlasAllSumDf.loc[ltlasAllSumDf.rBasic == np.inf, 'rFirst14']
    
    ltlasAllSumDf['relativeDiff'] = ltlasAllSumDf['rFirst14'] -  ltlasAllSumDf['rSecond14']
    ltlasAllSumDf['rate'] = np.round(ltlasAllSumDf['rate'], 1)
    ltlasAllSumDf['last30dRate'] = np.round(ltlasAllSumDf['last30dRate'], 1)
    ltlasAllSumDf['last7dRate'] = np.round(ltlasAllSumDf['last7dRate'], 1)
    ltlasAllSumDf['rFirst14'] = np.round(ltlasAllSumDf['rFirst14'], 1)
    ltlasAllSumDf['rSecond14'] = np.round(ltlasAllSumDf['rSecond14'], 1)
    ltlasAllSumDf['relativeDiff'] = np.round(ltlasAllSumDf['relativeDiff'], 1)
    ltlasAllSumDf['rBasic'] = np.round(ltlasAllSumDf['rBasic'], 1)    

    ltlasWorst10RateLast30D = ltlasDf[ltlasDf.specimenDate > ltlasDf.specimenDate.max(
    ) - timedelta(days=30)].groupby(['areaCode','areaName'])['rate'].sum().sort_values(ascending=False).head(10).reset_index()
    ltlasWorst10RateLast30D.rate = np.round(ltlasWorst10RateLast30D.rate, 1)

    ltlasSumDf = ltlasDf.groupby(
        'areaCode')['dailyLabConfirmedCases'].sum().reset_index()
    tmp = ltlasDf[ltlasDf.specimenDate > (ltlasDf.specimenDate.max(
    ) - timedelta(days=30))].groupby('areaCode')['dailyLabConfirmedCases'].sum()
    ltlasSumDf['last30dCases'] = ltlasSumDf['areaCode'].map(tmp)
    tmp = ltlasDf[(ltlasDf.specimenDate >= (ltlasDf.specimenDate.max() - timedelta(days=17))) & (ltlasDf.specimenDate <=
                                                                                                (ltlasDf.specimenDate.max() - timedelta(days=4)))].groupby('areaCode')['dailyLabConfirmedCases'].sum()
    tmp1 = ltlasDf[(ltlasDf.specimenDate >= (ltlasDf.specimenDate.max() - timedelta(days=31))) & (ltlasDf.specimenDate <=
                                                                                                (ltlasDf.specimenDate.max() - timedelta(days=18)))].groupby('areaCode')['dailyLabConfirmedCases'].sum()
    ltlasSumDf['rFirst14'] = ltlasSumDf['areaCode'].map(tmp)
    ltlasSumDf['rSecond14'] = ltlasSumDf['areaCode'].map(tmp1)
    ltlasSumDf['rBasic'] = np.round(
        ltlasSumDf['rFirst14'] / ltlasSumDf['rSecond14'], 2)
    ltlasSumDf.dailyLabConfirmedCases = ltlasSumDf.dailyLabConfirmedCases.astype(
        'int')
    ltlasSumDf.last30dCases = ltlasSumDf.last30dCases.astype('int')
    ltlasSumDf.rFirst14 = ltlasSumDf.rFirst14.astype('int')
    ltlasSumDf.rSecond14 = ltlasSumDf.rSecond14.astype('int')
    ltlasSumDf['rBasic'] = ltlasSumDf['rBasic'].fillna(0)
    #ltlasSumDf['rBasic'] = ltlasSumDf['rBasic'].replace(np.inf, ltlasSumDf['rFirst14'])
    ltlasSumDf.loc[ltlasSumDf.rBasic == np.inf, 'rBasic'] = ltlasSumDf.loc[ltlasSumDf.rBasic == np.inf, 'rFirst14']

    #tmp = lowerToUpperDf[['LTLA19CD', 'LTLA19NM']].drop_duplicates()
    #tmp = tmp.rename(columns={'LTLA19CD': 'areaCode', 'LTLA19NM': 'areaName'})
    tmp = ltlasAllSumDf[['areaCode', 'areaName']].drop_duplicates()
    tmp = pd.merge(
        left=ltlasSumDf,
        right=tmp,
        how='left')
    ltlasWorst10Df = tmp[tmp.rFirst14 >= 30][['areaCode','areaName', 'rFirst14',
                                            'rSecond14', 'rBasic']].sort_values(by='rBasic', ascending=False).head(10)
    ltlastop10last30dDf = tmp[['areaCode','areaName', 'last30dCases']].sort_values(
        by='last30dCases', ascending=False).head(10)

    ltlasDf = ltlasDf[ltlasDf.specimenDate >= '2020-03-01']

    ltlasDf = ltlasDf.rename(
        columns={
            "dailyLabConfirmedCases": "dcLower",
            "totalLabConfirmedCases": "tcLower",
            "areaMovingAverage7": "ma7Lower",
        }
    )
    return ltlasDf, ltlasSumDf, ltlasWorst10Df, ltlastop10last30dDf, ltlasWorst10RateLast30D, ltlasAllSumDf


def exportData(ltlasDf, ltlasSumDf, ltlasWorst10Df, ltlastop10last30dDf, ltlasWorst10RateLast30D, ltlasAllSumDf, lastRefresh,msoaDf):
    #ltlasDf.to_csv('ltlas.csv')
    ltlasDf.to_json(path_or_buf="data/ltlas.json",
                    orient="records", date_format='iso')
    #ltlasSumDf.to_csv('ltlasSum.csv')
    ltlasSumDf.to_json(path_or_buf="data/ltlasSum.json",
                       orient="records", date_format='iso')
    #ltlasWorst10Df.to_csv('ltlasWorst10Df.csv')
    ltlasWorst10Df.to_json(
        path_or_buf="data/ltlasWorst10Df.json", orient="records", date_format='iso')
    #ltlastop10last30dDf.to_csv('ltlastop10last30dDf.csv')
    ltlastop10last30dDf.to_json(
        path_or_buf="data/ltlastop10last30d.json", orient="records", date_format='iso')
    #ltlasWorst10RateLast30D.to_csv('ltlasWorst10RateLast30D.csv')
    ltlasWorst10RateLast30D.to_json(
        path_or_buf="data/ltlasWorst10RateLast30D.json", orient="records", date_format='iso')
    #ltlasAllSumDf.to_csv('ltlasAllSumDf.csv')
    ltlasAllSumDf.to_json(
        path_or_buf="data/ltlasAllSumDf.json", orient="records", date_format='iso')
    timestr = time.strftime("%Y%m%d-%H%M%S")
    msoaDf.to_json(
        path_or_buf=r"C:\Users\Projects\Documents\Archive\msoaDf_"+timestr+".json", orient="records", date_format='iso')        

    msoaDf.to_json(
        path_or_buf="data/msoaDf.json", orient="records", date_format='iso')        
    lastRefresh.to_json(
        path_or_buf="data/lastRefresh.json" , orient="records", date_format='iso')



if __name__ == "__main__":
    ltlasDf, lastRefresh = getCasesData()
    population, lowerToUpperDf = getGeoData()
    msoaDf = getMSOAData()
    ltlasDf, ltlasSumDf, ltlasWorst10Df, ltlastop10last30dDf, ltlasWorst10RateLast30D, ltlasAllSumDf = processData(
        ltlasDf, population, lowerToUpperDf)
    exportData(ltlasDf, ltlasSumDf, ltlasWorst10Df,
               ltlastop10last30dDf, ltlasWorst10RateLast30D, ltlasAllSumDf, lastRefresh, msoaDf)
