import pandas as pd
import requests
import json
from datetime import timedelta
import numpy as np


def getCasesData():
    request = requests.get(
        "https://coronavirus.data.gov.uk/downloads/json/coronavirus-cases_latest.json")
    requestJson = json.loads(request.content)
    ltlasDf = pd.DataFrame(requestJson['ltlas'])
    return ltlasDf


def getGeoData():
    lowerToUpperDf = pd.read_csv(
        "data/Lower_Tier_Local_Authority_to_Upper_Tier_Local_Authority.csv")
    lowerToRegionDf = pd.read_csv(
        "data/Local_Authority_District_to_Region.csv")
    population = pd.read_csv("data/population.csv")
    return lowerToUpperDf, lowerToRegionDf, population


def processData(ltlasDf, lowerToUpperDf, lowerToRegionDf, population):
    ltlasDf.specimenDate = pd.to_datetime(ltlasDf.specimenDate)

    ltlasDf = ltlasDf[['areaCode', 'areaName', 'specimenDate',
                       'dailyLabConfirmedCases', 'totalLabConfirmedCases']]

    df1 = ltlasDf.drop_duplicates(subset=['areaCode', 'areaName'])[
        ['areaCode', 'areaName']]
    dtDf = pd.DataFrame(pd.date_range(ltlasDf.specimenDate.min(
    ), ltlasDf.specimenDate.max(), freq='1 D'), columns=['specimenDate'])

    df1['key'] = 0
    dtDf['key'] = 0

    df1 = df1.merge(dtDf, how='outer').drop(columns=['key'], axis=1)
    ltlasDf = df1.merge(ltlasDf, how='left', on=[
                        'areaCode', 'areaName', 'specimenDate']).fillna(0)

    ltlasDf = pd.merge(left=ltlasDf,
                       right=lowerToUpperDf,
                       how="left",
                       left_on="areaCode",
                       right_on="LTLA19CD")
    ltlasDf = ltlasDf.drop(['LTLA19CD', 'LTLA19NM', 'FID'], axis=1)
    ltlasDf = ltlasDf.rename(
        columns={"UTLA19CD": "upperRegionCode", 'UTLA19NM': "upperRegionName"})

    ltlasDf = pd.merge(left=ltlasDf,
                       right=ltlasDf.groupby(['specimenDate', 'upperRegionCode'])[
                           'dailyLabConfirmedCases'].sum().reset_index(),
                       how="left",
                       left_on=['specimenDate', 'upperRegionCode'],
                       right_on=['specimenDate', 'upperRegionCode'],
                       suffixes=["", "UpperRegion"])

    ltlasDf = pd.merge(left=ltlasDf,
                       right=lowerToRegionDf,
                       how="left",
                       left_on="areaCode",
                       right_on="LAD19CD")
    ltlasDf = ltlasDf.drop(['LAD19CD', 'LAD19NM', 'FID'], axis=1)
    ltlasDf = ltlasDf.rename(
        columns={"RGN19CD": "regionCode", 'RGN19NM': "regionName"})

    ltlasDf = pd.merge(left=ltlasDf,
                       right=ltlasDf.groupby(['specimenDate', 'regionCode'])[
                           'dailyLabConfirmedCases'].sum().reset_index(),
                       how="left",
                       left_on=['specimenDate', 'regionCode'],
                       right_on=['specimenDate', 'regionCode'],
                       suffixes=["", "Region"])

    ltlasDf.dailyLabConfirmedCases = ltlasDf.dailyLabConfirmedCases.fillna(0)

    ltlasDf['areaMovingAverage7'] = ltlasDf.groupby(
        'areaCode')['dailyLabConfirmedCases'].transform(lambda x: x.rolling(7, 1).mean())
    ltlasDf['upperRegionMovingAverage7'] = ltlasDf.groupby('upperRegionCode')[
        'dailyLabConfirmedCasesUpperRegion'].transform(lambda x: x.rolling(7, 1).mean())
    ltlasDf['regionMovingAverage7'] = ltlasDf.groupby('regionCode')[
        'dailyLabConfirmedCasesRegion'].transform(lambda x: x.rolling(7, 1).mean())

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
    ltlasAllSumDf['rBasic'] = ltlasAllSumDf['rBasic'].replace(np.inf, ltlasAllSumDf['rFirst14'])
    
    ltlasAllSumDf['relativeDiff'] = ltlasAllSumDf['rFirst14'] -  ltlasAllSumDf['rSecond14']


    ltlasWorst10RateLast30D = ltlasDf[ltlasDf.specimenDate > ltlasDf.specimenDate.max(
    ) - timedelta(days=30)].groupby(['areaName'])['rate'].sum().sort_values(ascending=False).head(10).reset_index()
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
    ltlasSumDf['rBasic'] = ltlasSumDf['rBasic'].replace(
        np.inf, ltlasSumDf['rFirst14'])

    tmp = lowerToUpperDf[['LTLA19CD', 'LTLA19NM']].drop_duplicates()
    tmp = tmp.rename(columns={'LTLA19CD': 'areaCode', 'LTLA19NM': 'areaName'})
    tmp = pd.merge(
        left=ltlasSumDf,
        right=tmp,
        how='left')
    ltlasWorst10Df = tmp[tmp.rFirst14 >= 30][['areaName', 'rFirst14',
                                            'rSecond14', 'rBasic']].sort_values(by='rBasic', ascending=False).head(10)
    ltlastop10last30dDf = tmp[['areaName', 'last30dCases']].sort_values(
        by='last30dCases', ascending=False).head(10)

    ltlasDf = ltlasDf[ltlasDf.specimenDate >= '2020-03-01']

    ltlasDf = ltlasDf.rename(
        columns={
            "dailyLabConfirmedCases": "dcLower",
            "totalLabConfirmedCases": "tcLower",
            "upperRegionCode": "urCode",
            "upperRegionName": "urName",
            "dailyLabConfirmedCasesUpperRegion": "dcUpper",
            "regionCode": "rCode",
            "regionName": "rName",
            "dailyLabConfirmedCasesRegion": "dcRegion",
            "areaMovingAverage7": "ma7Lower",
            "upperRegionMovingAverage7": "ma7Upper",
            "regionMovingAverage7": "ma7Region"
        }
    )
    return ltlasDf, ltlasSumDf, ltlasWorst10Df, ltlastop10last30dDf, ltlasWorst10RateLast30D, ltlasAllSumDf


def exportData(ltlasDf, ltlasSumDf, ltlasWorst10Df, ltlastop10last30dDf, ltlasWorst10RateLast30D, ltlasAllSumDf):
    ltlasDf.to_csv('ltlas.csv')
    ltlasDf.to_json(path_or_buf="data/ltlas.json",
                    orient="records", date_format='iso')
    ltlasSumDf.to_csv('ltlasSum.csv')
    ltlasSumDf.to_json(path_or_buf="data/ltlasSum.json",
                       orient="records", date_format='iso')
    ltlasWorst10Df.to_csv('ltlasWorst10Df.csv')
    ltlasWorst10Df.to_json(
        path_or_buf="data/ltlasWorst10Df.json", orient="records", date_format='iso')
    ltlastop10last30dDf.to_csv('ltlastop10last30dDf.csv')
    ltlastop10last30dDf.to_json(
        path_or_buf="data/ltlastop10last30d.json", orient="records", date_format='iso')
    ltlasWorst10RateLast30D.to_csv('ltlasWorst10RateLast30D.csv')
    ltlasWorst10RateLast30D.to_json(
        path_or_buf="data/ltlasWorst10RateLast30D.json", orient="records", date_format='iso')
    ltlasAllSumDf.to_csv('ltlasAllSumDf.csv')
    ltlasAllSumDf.to_json(
        path_or_buf="data/ltlasAllSumDf.json", orient="records", date_format='iso')


if __name__ == "__main__":
    ltlasDf = getCasesData()
    lowerToUpperDf, lowerToRegionDf, population = getGeoData()
    ltlasDf, ltlasSumDf, ltlasWorst10Df, ltlastop10last30dDf, ltlasWorst10RateLast30D, ltlasAllSumDf = processData(
        ltlasDf, lowerToUpperDf, lowerToRegionDf, population)
    exportData(ltlasDf, ltlasSumDf, ltlasWorst10Df,
               ltlastop10last30dDf, ltlasWorst10RateLast30D, ltlasAllSumDf)
