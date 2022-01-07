# -*- coding: utf-8 -*-
#################################################################################################################################
## imports functions ############################################################################################################
#################################################################################################################################

import sys as sys
sys.path[0]=''
import numpy as np
import pandas as pd
import os as os
import datetime as dt
from scipy import interpolate
import os.path
import datetime

#################################################################################################################################
## defining functions ###########################################################################################################
#################################################################################################################################


def groupby_count_desc(df, columns, ascending = False):
    """
    returns the data frame grouped by the columns with a count of how of then this grouping occurs
    sorted 
    """
    return df.groupby(
    columns, dropna=False).size().reset_index(name = "count").sort_values(by = "count", ascending = ascending)

def power_bi_type_cast(df):
    type_string = '= Table.TransformColumnTypes(#"Promoted Headers",\n{   \n'
    first = True
    
    
    max_len_c_name = len(max(df.columns, key=len))
    
    for i_c in df.dtypes.iteritems():
        c_name = i_c[0]
        c_type = i_c[1]        
        
        if first:
            type_string += ' ' 
            first = False
        else:
            type_string += ','
            
        type_string += '{"'+c_name+'" '       
        type_string +=' '*(max_len_c_name-len(c_name)) # Ensures that all types start at the same point making it easiert to read
        type_string +=', '
        
        # python type to Power_BI type
        if c_type in ['object','bool']:
            type_string += 'type text'
        elif c_type in ['int64', 'int32']:
            type_string += 'Int64.Type'
        elif c_type in ['float64']:
            type_string += 'type number'
        elif c_type in ['<M8[ns]', 'datetime64[ns]']:
            type_string += 'type date'
        else:
            type_string += 'ERROR'
        type_string += '}\n'        
    type_string += '})\n'
    return type_string

#################################################################################################################################
## reading data #################################################################################################################
#################################################################################################################################

# data_stringency = pd.read_csv('https://raw.githubusercontent.com/OxCGRT/covid-policy-tracker/master/data/OxCGRT_latest.csv')
data_rki        = pd.read_csv('https://www.arcgis.com/sharing/rest/content/items/f10774f1c63e40168479a1feb6c7ca74/data')
data_rki.to_csv("source_data_rki.tsv"  , index = False, sep = '\t', encoding='utf-8-sig', line_terminator='\r\n')
# data_mobility   = pd.read_csv('2020_DE_Region_Mobility_Report.csv')
data_bl_mapping = pd.read_csv('IdBundesland_iso.txt', sep = '\t', encoding = 'latin_1')
data_kreise_cat = pd.read_csv('kreise_category.txt', sep = '\t', encoding = 'latin_1') \
    .assign(
        kreis_id = lambda x: x.kreis_id.apply(lambda v: str(v).rjust(5,'0'))
    )
data_kreise     = pd.read_csv('kreise.txt', sep = '\t', encoding = 'latin_1') \
    .assign(
        rs = lambda x: x.rs.apply(lambda v: str(v).rjust(5,'0'))
    )

#################################################################################################################################
## transforming data ############################################################################################################  
#################################################################################################################################

## rki data 1 ###################################################################################################################

geo_vars = [
      'IdBundesland'
    , 'Bundesland'
    , 'IdLandkreis'
    , 'Landkreis'
]

geo_time_vars = geo_vars + ['Meldedatum'] 

rki_pk_vars = geo_time_vars + [
      'Altersgruppe'
    , 'Geschlecht'
]

value_vars = [
      'AnzahlFall'
    , 'AnzahlTodesfall'
    , 'AnzahlGenesen'
]

data_rki.columns
data_rki.columns
data_rki_agg = data_rki \
    .loc[:, rki_pk_vars + value_vars] \
    .groupby(rki_pk_vars) \
    .sum() \
    .reset_index() \
    .assign(
        IdLandkreis  = lambda x: x.IdLandkreis.apply(lambda v: str(v).rjust(5,'0')).replace({
          '05354': '05334',
        })
      , Landkreis    = lambda x: x.Landkreis.replace({
          'LK Göttingen (alt)': 'LK Göttingen',
          'LK Aachen'         : 'StadtRegion Aachen'
        }) 
      , Meldedatum   = lambda x: x.Meldedatum.apply(lambda v: datetime.datetime.strptime(v, '%Y/%m/%d %H:%M:%S'))
      , AnzahlAktiv  = lambda x: x.AnzahlFall.fillna(0) - x.AnzahlTodesfall.fillna(0) - x.AnzahlGenesen.fillna(0)
    ) \
    .melt(
      id_vars    = rki_pk_vars
    , value_vars = value_vars + ['AnzahlAktiv']

    , var_name   = 'Status'
    , value_name = 'Anzahl'
    ) \
    .loc[lambda df: df.Status != 'AnzahlFall'] \
    .assign(
        Status = lambda x: x.Status.apply(lambda v: v.replace('Anzahl', ''))
    ) \
    .groupby(rki_pk_vars + ['Status']) \
    .sum() \
    .reset_index()

## general data #################################################################################################################

data_geo = data_rki_agg \
    .loc[:,['IdBundesland', 'Bundesland', 'IdLandkreis', 'Landkreis']] \
    .drop_duplicates() \
    .merge(
        data_kreise[['rs', 'population']]
      , how      = 'outer'
      , left_on  = 'IdLandkreis'
      , right_on = 'rs'
      , validate = '1:1'
    ) \
    .merge(
        data_kreise_cat
      , how      = 'outer'
      , left_on  = 'IdLandkreis'
      , right_on = 'kreis_id'
      , validate = '1:1'
    ) \
    .assign(ones = 1) \
    .drop(columns = ['rs', 'kreis_id', 'krs17'])\
    .loc[lambda df: df["IdLandkreis"].notna()]
    

min_Datum = min(data_rki_agg.Meldedatum)     
max_Datum = max(data_rki_agg.Meldedatum)     
 
data_time =  pd.date_range(start=min_Datum, end=max_Datum) \
    .to_frame(name = 'Datum') \
    .reset_index() \
    .drop(columns = ['index']) \
    .assign(
        id_time = lambda x: x.index
      , ones = 1
      , time_cat = lambda x: \
        np.where(x.Datum < dt.datetime(2020, 3, 1), '1: Bis Februar',
        np.where(x.Datum < dt.datetime(2020, 6, 1), '2: März - Mai',
        np.where(x.Datum < dt.datetime(2020,10, 1), '3: Juni - September',
        np.where(x.Datum < max_Datum - dt.timedelta(days= 27), '4: Oktober - vor 4 Wochen',
        np.where(x.Datum < max_Datum - dt.timedelta(days= 6), '5: vor 4 Wochen - vor 1er Woche', '6: letze Woche'
        )))))
    )    

data_geo_time_prep = data_geo \
    .loc[:,['IdBundesland', 'Bundesland', 'IdLandkreis', 'Landkreis', 'ones']] \
    .merge(data_time, on = "ones") \
    .reset_index() \
    .drop(columns = ['index']) \
    .assign(
        id_lk_time = lambda x: x.index
    )

df_bl_time = data_geo_time_prep \
    .loc[:,['IdBundesland', 'Bundesland', 'Datum', 'id_time']] \
    .drop_duplicates() \
    .reset_index() \
    .assign(
        id_bl_time = lambda x: x.index
    )

data_geo_time = data_geo_time_prep \
    .merge(
        df_bl_time
      , on = ['IdBundesland', 'Bundesland', 'Datum', 'id_time']
    )

df_age = data_rki_agg \
    .loc[:,['Altersgruppe']] \
    .drop_duplicates() \
    .assign(ones = 1) 

df_sex = data_rki_agg \
    .loc[:,['Geschlecht']] \
    .drop_duplicates() \
    .assign(ones = 1) 

df_status = data_rki_agg \
    .loc[:,['Status']] \
    .drop_duplicates() \
    .assign(ones = 1) 

df_rki_pk_vars_span = data_geo_time \
    .merge(df_age   , on = "ones") \
    .merge(df_sex   , on = "ones") \
    .merge(df_status, on = "ones")


## rki data 2 ###################################################################################################################

data_rki_export = df_rki_pk_vars_span \
    .merge(
        data_rki_agg
      , how      = 'left'
      , left_on  = ['IdLandkreis', 'IdBundesland', 'Datum'     , 'Altersgruppe', 'Geschlecht', 'Status']
      , right_on = ['IdLandkreis', 'IdBundesland', 'Meldedatum', 'Altersgruppe', 'Geschlecht', 'Status']
      , validate = 'm:1'      
      ) \
      .assign(
        Anzahl       = lambda x: x.Anzahl.fillna(0)
      , Geschlecht   = lambda x: x.Geschlecht.replace('unbekannt', 'keine Information')
      , Altersgruppe = lambda x: x.Altersgruppe.replace('unbekannt', 'keine Information')
      , Alter_Zahl   = lambda x: x.Altersgruppe.replace({ 
                                                              'A00-A04'          : 2 
                                                            , 'A05-A14'          : 9.5
                                                            , 'A15-A34'          : 24.5
                                                            , 'A35-A59'          : 47
                                                            , 'A60-A79'          : 69.5
                                                            , 'A80+'             : 85
                                                            , 'keine Information': 44.5
                                                            }
                                                        )  
      ) \
    .loc[:,['IdBundesland', 'IdLandkreis','id_time', 'id_lk_time', 
            'id_bl_time', 'Altersgruppe', 'Alter_Zahl', 'Geschlecht', 
            'Status', 'Anzahl']] \

# ## mobility data ################################################################################################################  

# data_mobility_agg = data_mobility \
#     .merge(
#         data_bl_mapping
#       , how      = 'inner'
#       , on       = 'iso_3166_2_code'
#       , validate = 'm:1'
#     ) \
#     .assign(
#         Datum = lambda x: x.date.apply(lambda v: datetime.datetime.strptime(v, '%Y-%m-%d'))
#     ) \
#     .rename(columns={
#         'retail_and_recreation_percent_change_from_baseline' : 'Einzelhandel und Freizeit'
#       , 'grocery_and_pharmacy_percent_change_from_baseline'  : 'Lebensmittel und Apotheken'
#       , 'parks_percent_change_from_baseline'                 : 'Parks'
#       , 'transit_stations_percent_change_from_baseline'      : 'Öffentliche Verkehrsmittel'
#       , 'workplaces_percent_change_from_baseline'            : 'Arbeitsplatz'
#       , 'residential_percent_change_from_baseline'           : 'Wohnort'
#     }) \
#     .loc[:,[
#         'IdBundesland'
#       , 'sub_region_1'
#       , 'Datum'
#       , 'Einzelhandel und Freizeit'
#       , 'Lebensmittel und Apotheken'
#       , 'Parks'
#       , 'Öffentliche Verkehrsmittel'
#       , 'Arbeitsplatz'
#       , 'Wohnort'
#     ]] \
#     .merge(
#         df_bl_time
#       , on = ['IdBundesland', 'Datum']
#       , validate = '1:1'
#     )

# data_mobility_export = data_mobility_agg


# ## stringency data ################################################################################################################  
# data_stringency_agg = data_stringency \
#     .loc[lambda df: df.CountryName == 'Germany'] \
#     .assign(
#         Datum = lambda x: x.Date.apply(lambda v: datetime.datetime.strptime(str(v), '%Y%m%d'))
#     ) \
#     .merge(
#         data_time.drop(columns = ['ones'])
#       , on = ['Datum']
#       , validate = '1:1'
#     )
# data_stringency_export = data_stringency_agg

#################################################################################################################################
## export data ##################################################################################################################
#################################################################################################################################

f = open("date.txt", "w")
f.write(dt.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
f.close()


data_max_date = pd.DataFrame({'max_date': [max_Datum]})



print('data_max_date:'         , power_bi_type_cast(data_max_date         ),data_max_date         .shape)
print('data_rki_export:'       , power_bi_type_cast(data_rki_export       ),data_rki_export       .shape)
print('data_geo_time:'         , power_bi_type_cast(data_geo_time         ),data_geo_time         .shape)
print('data_time:'             , power_bi_type_cast(data_time             ),data_time             .shape)
print('data_geo:'              , power_bi_type_cast(data_geo              ),data_geo              .shape)
# print('data_mobility_export:'  , power_bi_type_cast(data_mobility_export  ),data_mobility_export  .shape)
# print('data_stringency_export:', power_bi_type_cast(data_stringency_export),data_stringency_export.shape)



data_max_date                                   .to_csv("data_max_date.tsv"  , index = False, sep = '\t', encoding='utf-8-sig', line_terminator='\r\n')
data_rki_export.loc[lambda df: (df.Anzahl != 0)].to_csv("data_rki.tsv"       , index = False, sep = '\t', encoding='utf-8-sig', line_terminator='\r\n')
data_geo_time                                   .to_csv("data_geo_time.tsv"  , index = False, sep = '\t', encoding='utf-8-sig', line_terminator='\r\n')
data_time                                       .to_csv("data_time.tsv"      , index = False, sep = '\t', encoding='utf-8-sig', line_terminator='\r\n')
data_geo                                        .to_csv("data_geo.tsv"       , index = False, sep = '\t', encoding='utf-8-sig', line_terminator='\r\n')
# data_mobility_export                            .to_csv("data_mobility.tsv"  , index = False, sep = '\t', encoding='utf-8-sig', line_terminator='\r\n')
# data_stringency_export                          .to_csv("data_stringency.tsv", index = False, sep = '\t', encoding='utf-8-sig', line_terminator='\r\n')


# data_rki_agg.loc[
#     lambda df: df.Anzahl < 0 
# ]
# data_rki.loc[
#       (data_rki['Meldedatum']   == '2020/07/15 00:00:00')
#     & (data_rki['IdLandkreis']  == 8436)
#     & (data_rki['Altersgruppe'] == 'A05-A14')
#     & (data_rki['Geschlecht']   == 'M')
#     ]


# data_rki_export.to_csv("data_rki.csv")

# data_rki_agg
# data_rki_agg.to_string().to_string()

# data_rki.IdLandkreis
# data_rki.loc[
#       (data_rki['Meldedatum']   == '2020/04/02 00:00:00')
#     & (data_rki['IdLandkreis']  == 8116)
#     & (data_rki['Altersgruppe'] == 'A80+')
#     & (data_rki['Geschlecht']   == 'W')
#     & (data_rki['Refdatum']   == '2020/04/02 00:00:00')
#     ].drop(columns=['ObjectId']).drop_duplicates().shape


# data_rki.loc[
#       (data_rki['Bundesland']  == 'Berlin'), ['Bundesland', 'Landkreis', 'IdLandkreis']
#     ].drop_duplicates()



# data_rki.shape
# 237 * 412 *3 * 7


# 245840
# 604836

# pd.set_option('display.max_rows', 1000)
# pd.set_option('display.max_columns', 50)

# data_path = os.path.join(os.getcwd(),'COVID-19','csse_covid_19_data')
# print(data_path)

# df_lu = pd.read_csv(os.path.join(data_path,'UID_ISO_FIPS_LookUp_Table.csv'))
# df_co = pd.read_csv(os.path.join(data_path,'csse_covid_19_time_series', 'time_series_covid19_confirmed_global.csv'))
# df_re = pd.read_csv(os.path.join(data_path,'csse_covid_19_time_series', 'time_series_covid19_recovered_global.csv'))
# df_de = pd.read_csv(os.path.join(data_path,'csse_covid_19_time_series', 'time_series_covid19_deaths_global.csv'))

# df_europe = pd.read_csv(os.path.join(os.getcwd(),  'european_countries.tsv'), sep='\t')

# df_owid = pd.read_csv(os.path.join(os.getcwd(),'covid-19-data', 'public', 'data', 'owid-covid-data.csv'))

# df_lu.columns
# df_co.columns
# df_re.columns
# df_de.columns

# country_regions_to_group = set([
#      "Canada"         #because of Problems within John Hopkins Dataset
#     ,"China"          #because of Problems merging with owid on iso3
#     ,"Australia"      #because of Problems merging with owid on iso3
#     ,"United Kingdom" #because of Problems merging with owid on iso3  
#     ])

# join_columns = ['country_region', 'province_state', 'date']

# df_co['Country/Region'].isin(country_regions_to_group)
# id_vars = ['Province/State', 'Country/Region', 'Lat', 'Long']

# def clean_df(df, id_vars):
#     value_vars = set(df.columns) - set(id_vars)
#     return pd.melt(
#         df
#         , id_vars=id_vars
#         , value_vars=value_vars
#         , var_name='date'
#         , value_name='number'
#     ).assign(
#         date              = lambda x: pd.to_datetime(x.date,format = '%m/%d/%y')
#         , province_state  = lambda x: np.where(x['Country/Region'].isin(country_regions_to_group), '', x['Province/State'].fillna(''))
#         , country_region  = lambda x: x['Country/Region']
#     )[join_columns + ['number']].groupby(join_columns).sum().reset_index()

# df_co_clean = clean_df(df_co, id_vars).assign(confirmed = lambda x: x.number).drop(['number'], axis=1)
# df_re_clean = clean_df(df_re, id_vars).assign(recovered = lambda x: x.number).drop(['number'], axis=1)
# df_de_clean = clean_df(df_de, id_vars).assign(deaths    = lambda x: x.number).drop(['number'], axis=1)

# assert df_co_clean[join_columns].equals(df_re_clean[join_columns]),  "df_co_clean[join_columns] != df_re_clean[join_columns]"
# assert df_re_clean[join_columns].equals(df_de_clean[join_columns]),  "df_re_clean[join_columns] != df_de_clean[join_columns]"

# # the map visuals need slightly different country names than found in country_region:
# df_country_map_names = pd.DataFrame(
# { 'country_region'    : ['US'                      ,'Congo (Brazzaville)', 'Congo (Kinshasa)','Burma'  ,"Cote d'Ivoire", 'South Sudan', 'Central African Republic','Korea, South']
# , 'country_region_map': ['United States of America','Congo'              , 'Dem. Rep. Congo' ,'Myanmar',"Côte d'Ivoire", 'S. Sudan'   , 'Central African Rep.'    ,'South Korea' ]  
# })

# df_lu_rename = df_lu.assign(
#       province_state  = lambda x: x['Province_State'].fillna('')
#     , country_region  = lambda x: x['Country_Region']
#     , lu_id           = lambda x: range(x.shape[0])
# ).drop(['Province_State', 'Country_Region'], axis=1).merge(
#     df_country_map_names, how = 'outer', on = ['country_region']
# ).merge(
#     df_europe, how = 'outer', on = ['iso2']
# ).assign(
#       country_region_map = lambda x: x.country_region_map.fillna(x.country_region)
#     , country_group = lambda x: x.country_group.fillna('')
# )

# unique_crps = df_co_clean[['country_region', 'province_state']].drop_duplicates().reset_index()

# # check that all country_regions are in lookup table
# assert df_lu_rename.merge(unique_crps, how = "inner").shape[0] == unique_crps.shape[0]
# df_lu_clean = df_lu_rename.merge(unique_crps, how = "inner")

# # only two rows that don't have iso3 (Diamond Princess and MS Zaandam	)
# assert df_lu_clean[df_lu_clean.iso3.isnull()].shape[0] == 2

# df_te = df_owid.merge(
#       df_lu_clean[df_lu_clean.iso3.notnull()]
#     , how = 'outer'
#     , left_on='iso_code'
#     , right_on='iso3'
#     # , validate="m:1"    
# ).assign(
#       iso_code        = lambda x: x.iso_code.fillna("missing")
#     , iso3            = lambda x: x.iso3.fillna("missing")
#     , location        = lambda x: x.location.fillna("missing")
#     , country_region  = lambda x: x.country_region.fillna("missing")
# )

# # #TODO: Fix this to make it empty
# # assert df_te.groupby([
# #   "iso_code"      
# # , "iso3"          
# # , "location"      
# # , "country_region"]).sum().reset_index().query(
# # "iso_code == 'missing' or iso3 == 'missing' or location == 'missing' or country_region == 'missing'"
# # ).shape[0] == 0

# df_te_clean = df_te.assign(
#       date            = lambda x: pd.to_datetime(x.date,format = '%Y-%m-%d')
#     , tested_reported = lambda x: x.total_tests.fillna(0)
# )[['lu_id', 'date', 'tested_reported']].groupby(['lu_id', 'date']).sum().reset_index().assign(
#       tested_reported_or_nan = lambda x: x.tested_reported.replace(0, np.nan)
# )

# df_collect_temps = pd.DataFrame()

# for lu in set(df_te_clean.lu_id):
#     df_temp = df_te_clean.loc[df_te_clean.lu_id == lu]
#     df_temp = df_temp.assign(
#           counter           = lambda x: range(len(x))
#         , tested_announced  = lambda x: x.tested_reported_or_nan.interpolate(method='pad', limit_direction='forward', limit_area=None)
#     )
#     contains_tested = df_temp.tested_reported_or_nan.notnull().astype(int)
#     if sum(contains_tested) >= 2 :
#         x = df_temp[df_temp.tested_reported_or_nan.notnull()].counter
#         y = df_temp[df_temp.tested_reported_or_nan.notnull()].tested_reported_or_nan
#         f = interpolate.interp1d(x, y, fill_value='extrapolate')
#         df_temp = df_temp.assign(
#             tested = lambda x: f(x.counter).clip(0, None)
#         )
#     else:
#         df_temp = df_temp.assign(
#             tested = lambda x: x.tested_announced
#         )
#     df_collect_temps = df_collect_temps.append(df_temp)

# df_te_clean_est = df_te_clean.merge(
#       df_collect_temps[['lu_id', 'date', 'tested_announced', 'tested']]
#     , how = 'left'
#     , on = ['lu_id', 'date']).assign(
#           tested_announced    = lambda x: x.tested_announced.fillna(0).round() 
#         , tested              = lambda x: x.tested.fillna(0).round()            #tested = tested_announced + tested_extrapolated 
#         , tested_extrapolated    = lambda x: (x.tested - x.tested_announced).fillna(0).round()
#         , tested_is_extrapolated = lambda x: np.where(x.tested_reported_or_nan.isnull() ,1 ,0 )
#     )[['lu_id', 'date', 'tested_reported', 'tested_announced', 'tested_extrapolated', 'tested','tested_is_extrapolated']]


# df_data_clean = df_co_clean.merge(
#     df_re_clean    , how = 'inner', on = join_columns).merge(                          #, validate = "1:1"  
#     df_de_clean    , how = 'inner', on = join_columns).merge(                          #, validate = "1:1"  
#     df_lu_clean    , how = 'inner', on = ['country_region', 'province_state']).merge(  #, validate = "m:1"                          
#     df_te_clean_est, how = 'inner', on = ['lu_id','date'])[                            #, validate = "1:1" 
#         ["lu_id", 'country_region', 'province_state', 'date', 'confirmed', 'recovered', 'deaths'
#         , 'tested', 'tested_reported', 'tested_announced', 'tested_extrapolated','tested_is_extrapolated']
#     ].query("date.notnull()", engine = "python"
#     ).assign(                           
#           lu_id = lambda x: x.lu_id.fillna(-1)
#         , confirmed           = lambda x: x['confirmed'].fillna(0)
#         , recovered           = lambda x: x['recovered'].fillna(0)
#         , deaths              = lambda x: x['deaths'   ].fillna(0)
#         , tested              = lambda x: x['tested'   ].fillna(0)
#     ).assign(
#           lag_21_confirmed    = lambda x: x.sort_values(by=['date'], ascending=True).groupby(['lu_id'])['confirmed'].shift(21).fillna(0)
#     ).assign(
#           probably_recovered  = lambda x: np.maximum(np.minimum(x['lag_21_confirmed'],x['confirmed']) - x['recovered'] - x['deaths'], 0)
#     ).assign(
#           active              = lambda x: (x.confirmed - x.deaths - x.recovered - x.probably_recovered).fillna(0)
#     ).assign(
#         lag_1_confirmed              = lambda x: x.sort_values(by=['date'], ascending=True).groupby(['lu_id'])['confirmed'             ].shift(1).fillna(0)
#     ,   lag_1_recovered              = lambda x: x.sort_values(by=['date'], ascending=True).groupby(['lu_id'])['recovered'             ].shift(1).fillna(0)
#     ,   lag_1_deaths                 = lambda x: x.sort_values(by=['date'], ascending=True).groupby(['lu_id'])['deaths'                ].shift(1).fillna(0)
#     ,   lag_1_tested                 = lambda x: x.sort_values(by=['date'], ascending=True).groupby(['lu_id'])['tested'                ].shift(1).fillna(0)
#     ,   lag_1_active                 = lambda x: x.sort_values(by=['date'], ascending=True).groupby(['lu_id'])['active'                ].shift(1).fillna(0)
#     ,   lag_1_tested_is_extrapolated = lambda x: x.sort_values(by=['date'], ascending=True).groupby(['lu_id'])['tested_is_extrapolated'].shift(1).fillna(0)
#     ,   lag_7_confirmed              = lambda x: x.sort_values(by=['date'], ascending=True).groupby(['lu_id'])['confirmed'             ].shift(7).fillna(0)
#     ,   lag_7_recovered              = lambda x: x.sort_values(by=['date'], ascending=True).groupby(['lu_id'])['recovered'             ].shift(7).fillna(0)
#     ,   lag_7_deaths                 = lambda x: x.sort_values(by=['date'], ascending=True).groupby(['lu_id'])['deaths'                ].shift(7).fillna(0)
#     ,   lag_7_tested                 = lambda x: x.sort_values(by=['date'], ascending=True).groupby(['lu_id'])['tested'                ].shift(7).fillna(0)
#     ,   lag_7_active                 = lambda x: x.sort_values(by=['date'], ascending=True).groupby(['lu_id'])['active'                ].shift(7).fillna(0)
#     ,   lag_7_tested_is_extrapolated = lambda x: x.sort_values(by=['date'], ascending=True).groupby(['lu_id'])['tested_is_extrapolated'].shift(7).fillna(0)
#     )

# max_date = max(df_data_clean.date)  

# df_lu_clean.to_csv(                             "df_lu_clean.tsv"           , index = False, sep = '\t', encoding='utf-8-sig')
# df_data_clean.to_csv(                           "df_data_clean.tsv"         , index = False, sep = '\t' ,encoding='utf-8-sig')
# df_data_clean.query("date == @max_date").to_csv("df_data_clean_max_date.tsv", index = False, sep = '\t' ,encoding='utf-8-sig')


# f = open("date.txt", "w")
# f.write(dt.datetime.now().strftime("%d/%m/%Y %H:%M:%S"))
# f.close()


# print(df_lu_clean.shape)
# print(df_data_clean.shape)

# def power_bi_type_cast(df):
#     type_string = '= Table.TransformColumnTypes(#"Promoted Headers",\n{   \n'
#     first = True
    
    
#     max_len_c_name = len(max(df.columns, key=len))
    
#     for i_c in df.dtypes.iteritems():
#         c_name = i_c[0]
#         c_type = i_c[1]        
        
#         if first:
#             type_string += ' ' 
#             first = False
#         else:
#             type_string += ','
            
#         type_string += '{"'+c_name+'" '       
#         type_string +=' '*(max_len_c_name-len(c_name)) # Ensures that all types start at the same point making it easiert to read
#         type_string +=', '
        
#         # python type to Power_BI type
#         if c_type in ['object','bool']:
#             type_string += 'type text'
#         elif c_type in ['int64', 'int32']:
#             type_string += 'Int64.Type'
#         elif c_type in ['float64']:
#             type_string += 'type number'
#         elif c_type in ['<M8[ns]']:
#             type_string += 'type date'
#         else:
#             type_string += 'ERROR'
#         type_string += '}\n'        
#     type_string += '})\n'
#     return type_string


# print(power_bi_type_cast(df_lu_clean))
# print(power_bi_type_cast(df_data_clean))




# # daily growth active prev day            = COALESCE(DIVIDE(SUM('data_at'[active]   ), SUM('data_at'[lag_1_active]   )),1)       - 1
# # daily growth confirmed prev day         = COALESCE(DIVIDE(SUM('data_at'[confirmed]), SUM('data_at'[lag_1_confirmed])),1)       - 1
# # daily growth deaths prev day            = COALESCE(DIVIDE(SUM('data_at'[deaths]   ), SUM('data_at'[lag_1_deaths]   )),1)       - 1
# # daily growth recovered prev day         = COALESCE(DIVIDE(SUM('data_at'[recovered]), SUM('data_at'[lag_1_recovered])),1)       - 1
# # daily growth active prev week           = COALESCE(DIVIDE(SUM('data_at'[active]   ), SUM('data_at'[lag_7_active]   )),1)^(1/7) - 1      
# # daily growth confirmed prev week        = COALESCE(DIVIDE(SUM('data_at'[confirmed]), SUM('data_at'[lag_7_confirmed])),1)^(1/7) - 1      
# # daily growth deaths prev week           = COALESCE(DIVIDE(SUM('data_at'[deaths]   ), SUM('data_at'[lag_7_deaths]   )),1)^(1/7) - 1      
# # daily growth recovered prev week        = COALESCE(DIVIDE(SUM('data_at'[recovered]), SUM('data_at'[lag_7_recovered])),1)^(1/7) - 1      
# # hist daily growth active prev day       = COALESCE(DIVIDE(SUM('data_ot'[active]   ), SUM('data_ot'[lag_1_active]   )),1)       - 1
# # hist daily growth confirmed prev day    = COALESCE(DIVIDE(SUM('data_ot'[confirmed]), SUM('data_ot'[lag_1_confirmed])),1)       - 1
# # hist daily growth deaths prev day       = COALESCE(DIVIDE(SUM('data_ot'[deaths]   ), SUM('data_ot'[lag_1_deaths]   )),1)       - 1
# # hist daily growth recovered prev day    = COALESCE(DIVIDE(SUM('data_ot'[recovered]), SUM('data_ot'[lag_1_recovered])),1)       - 1
# # hist daily growth active prev week      = COALESCE(DIVIDE(SUM('data_ot'[active]   ), SUM('data_ot'[lag_7_active]   )),1)^(1/7) - 1
# # hist daily growth confirmed prev week   = COALESCE(DIVIDE(SUM('data_ot'[confirmed]), SUM('data_ot'[lag_7_confirmed])),1)^(1/7) - 1
# # hist daily growth deaths prev week      = COALESCE(DIVIDE(SUM('data_ot'[deaths]   ), SUM('data_ot'[lag_7_deaths]   )),1)^(1/7) - 1
# # hist daily growth recovered prev week   = COALESCE(DIVIDE(SUM('data_ot'[recovered]), SUM('data_ot'[lag_7_recovered])),1)^(1/7) - 1


# # positive test rate total           = DIVIDE(SUM('data_at'[confirmed])                                , SUM('data_at'[tested])                               )                                
# # positive test rate prev day        = DIVIDE(SUM('data_at'[confirmed])-SUM('data_at'[lag_1_confirmed]), SUM('data_at'[tested]) - SUM('data_at'[lag_1_tested]))
# # positive test rate prev week       = DIVIDE(SUM('data_at'[confirmed])-SUM('data_at'[lag_7_confirmed]), SUM('data_at'[tested]) - SUM('data_at'[lag_7_tested]))
# # hist positive test rate            = DIVIDE(SUM('data_ot'[confirmed])                                , SUM('data_ot'[tested])                               )                                
# # hist positive test rate prev day   = DIVIDE(SUM('data_ot'[confirmed])-SUM('data_ot'[lag_1_confirmed]), SUM('data_ot'[tested]) - SUM('data_ot'[lag_1_tested]))
# # hist positive test rate prev week  = DIVIDE(SUM('data_ot'[confirmed])-SUM('data_ot'[lag_7_confirmed]), SUM('data_ot'[tested]) - SUM('data_ot'[lag_7_tested]))'