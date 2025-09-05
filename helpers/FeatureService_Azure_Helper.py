'''
Author: Carter Hughes
Date: 20260306
Purpose: Help draft parameters, T-SQL, etc. for querying data originating from a Feature or Map Service in Azure
'''

# In[1]:


from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from functools import reduce
gis = GIS(profile="work")


# In[48]:
IGNORED_FIELDS = [
    'X',
    'Y',
    'LONGITUDE',
    'LATITUDE',
    'AREA',
    'ASSET_TYPE',
    'Shape__Area',
    'Shape__Length',
]

# In[49]:


def get_featurelayer_fields(
    featurelayer_url : str,
    gis : GIS
) -> list:
    return FeatureLayer(featurelayer_url, gis=gis).properties['fields']

def get_field_names(
    fields : list
) -> list:
    return [field['name'] for field in fields]

def get_featurelayer_field_names(
    featurelayer_url : str,
    gis : GIS
) -> str:
    return ', '.join([field['name'] for field in get_featurelayer_fields(featurelayer_url=featurelayer_url, gis=gis) if field['name'] not in IGNORED_FIELDS])

def get_featurelayer_bronzesqlfields(
    featurelayer_url : str,
    gis : GIS,
    name : str
) -> str:
    fields = get_featurelayer_fields(featurelayer_url=featurelayer_url, gis=gis)
    type_mappings = {
        'esriFieldTypeString': lambda field: f"VARCHAR({field['length']})",
        'esriFieldTypeSmallInteger': lambda field: 'INT',
        'esriFieldTypeBigInteger': lambda field: 'BIGINT',
        'esriFieldTypeInteger': lambda field: 'INT',
        'esriFieldTypeOID': lambda field: 'INT',
        'esriFieldTypeGlobalID': lambda field: "CHAR(36)",
        'esriFieldTypeGUID': lambda field: "CHAR(36)",
        'esriFieldTypeDate': lambda field: 'INT',
        'esriFieldTypeDouble' : lambda field: "NUMERIC(38, 8)",
        'esriFieldTypeFloat' : lambda field: "NUMERIC(12, 6)",
        'esriFieldTypeSingle' : lambda field: "NUMERIC(12, 6)",
        'esriFieldTypeBlob': lambda field: 'BINARY'
    }
    start_sql = f'''USE mds_ldw
GO
DROP EXTERNAL TABLE bronze.B_GIS_{name.upper()}
GO
CREATE EXTERNAL TABLE bronze.B_GIS_{name.upper()}
(
'''
    sql = ''
    for field in fields:
        if field['name'] not in IGNORED_FIELDS:
            sql += '    ,[' + field['name'] + '] VARCHAR(8000)' + '''
'''
    sql = start_sql + '    ' + sql[5:-1] + f'''
    ,[INGEST_FILE] VARCHAR(8000)
    ,[INGEST_TS] VARCHAR(8000)
    ,[GEOMWKB] VARBINARY(MAX)
    ,[GEOMWKT] VARCHAR(MAX)
    ,[X] FLOAT
    ,[Y] FLOAT
    ,[LONGITUDE] FLOAT
    ,[LATITUDE] FLOAT
    ,[AREA] FLOAT
    ,[LENGTH] FLOAT
    ,[GEOMTYPE] VARCHAR(8000)
    ,[COUNCIL2021] VARCHAR(8000)
    ,[COUNCIL2021_DOMINANT] VARCHAR(8000)
    ,[COUNCIL2021_AREAS] VARCHAR(8000)
    ,[LEGISLATIVE2022] VARCHAR(8000)
    ,[LEGISLATIVE2022_DOMINANT] VARCHAR(8000)
    ,[LEGISLATIVE2022_AREAS] VARCHAR(8000)
    ,[CONGRESS2021] VARCHAR(8000)
    ,[CONGRESS2021_DOMINANT] VARCHAR(8000)
    ,[CONGRESS2021_AREAS] VARCHAR(8000)
    ,[PARKPOLICEBEAT] VARCHAR(8000)
    ,[PARKPOLICEBEAT_DOMINANT] VARCHAR(8000)
    ,[PARKPOLICEBEAT_AREAS] VARCHAR(8000)
    ,[REGIONALSERVICECENTER] VARCHAR(8000)
    ,[REGIONALSERVICECENTER_DOMINANT] VARCHAR(8000)
    ,[REGIONALSERVICECENTER_AREAS] VARCHAR(8000)
    ,[CENSUSTRACT2020] VARCHAR(8000)
    ,[CENSUSTRACT2020_DOMINANT] VARCHAR(8000)
    ,[CENSUSTRACT2020_AREAS] VARCHAR(8000)
    ,[CENSUSTRACT2010] VARCHAR(8000)
    ,[CENSUSTRACT2010_DOMINANT] VARCHAR(8000)
    ,[CENSUSTRACT2010_AREAS] VARCHAR(8000)
    ,[GEOM_PARK_CODE] VARCHAR(8000)
    ,[GEOM_PARK_CODE_DOMINANT] VARCHAR(8000)
    ,[GEOM_PARK_CODE_AREAS] VARCHAR(8000)
    ,[GEOM_PARK_CODE_NEAREST] VARCHAR(8000)
    ,[GEOM_PARK_CODE_NEARESTDISTANCE] FLOAT
    ,[GEOM_PARK_CODE_NEARESTAREAS] VARCHAR(8000)
)  
WITH (
    LOCATION = '/bronze/gis-bronze/{name}/**',
    DATA_SOURCE = mds_ldw_source,  
    FILE_FORMAT = raw_ion_parquet
)
GO

-- SELECT TOP 1 * FROM bronze.B_GIS_{name.upper()}
    '''

    return sql


def clean_field_name(name : str) -> str:
    rename_fields = {
        'SIZE_': 'SIZE',
        'CREATED_DATE': 'CreationDate',
        'UPDATED_DATE': 'EditDate',
        'CREATED_USER': 'Creator',
        'UPDATED_USER': 'Editor'
    }
    clean_name = reduce(lambda a, kv: a.replace(*kv), rename_fields.items(), name)
    clean_name = clean_name.upper()

    return clean_name

    
def get_featurelayer_silversqlfields(
    featurelayer_url : str,
    gis : GIS
) -> str:
    fields = get_featurelayer_fields(featurelayer_url=featurelayer_url, gis=gis)
    type_mappings = {
        'esriFieldTypeString': lambda field: f"CAST([{field['name']}] AS VARCHAR({field['length'] if field['length'] < 8000 else 'MAX'})) AS [{clean_field_name(field['name'])}]",
        'esriFieldTypeSmallInteger': lambda field: f"CAST(CAST([{field['name']}] AS FLOAT) AS INT) AS [{clean_field_name(field['name'])}]",
        'esriFieldTypeBigInteger': lambda field: f"CAST(CAST([{field['name']}] AS FLOAT) AS BIGINT) AS [{clean_field_name(field['name'])}]",
        'esriFieldTypeInteger': lambda field: f"CAST(CAST([{field['name']}] AS FLOAT) AS INT) AS [{clean_field_name(field['name'])}]",
        'esriFieldTypeOID': lambda field: f"CAST([{field['name']}] AS INT) AS [{clean_field_name(field['name'])}]",
        'esriFieldTypeGlobalID': lambda field: f"CAST([{field['name']}] AS CHAR(36)) AS [{clean_field_name(field['name'])}]",
        'esriFieldTypeGUID': lambda field: f"CAST([{field['name']}] AS CHAR(36)) AS [{clean_field_name(field['name'])}]",
        'esriFieldTypeDate': lambda field: f"""DATEADD(S, CAST([{field['name']}] AS FLOAT)/1000, '1970-01-01') AT TIME ZONE 'UTC' AT TIME ZONE 'Eastern Standard Time' AS [{clean_field_name(field['name'])}]""",
        'esriFieldTypeDouble' : lambda field: f"CAST([{field['name']}] AS NUMERIC(38, 8)) AS [{clean_field_name(field['name'])}]",
        'esriFieldTypeFloat' : lambda field: f"CAST([{field['name']}] AS NUMERIC(12, 6)) AS [{clean_field_name(field['name'])}]",
        'esriFieldTypeSingle' : lambda field: f"CAST([{field['name']}] AS NUMERIC(12, 6)) AS [{clean_field_name(field['name'])}]",
        'esriFieldTypeBlob': lambda field: f"CAST([{field['name']}] AS BINARY) AS [{clean_field_name(field['name'])}]"
    }
    sql = ''''''
    for field in fields:
        if field['name'] not in IGNORED_FIELDS and field['type'] != 'esriFieldTypeGeometry':
            sql += '    ,' + type_mappings[field['type']](field) + '''
'''
    sql = '    ' + sql[5:-1] + '''
    ,[INGEST_TS]
    ,[INGEST_FILE]
    ,[GEOMWKB]
    ,[GEOMWKT]
    ,ROUND([X], 8) AS [X]
    ,ROUND([Y], 8) AS [Y]
    ,ROUND([LONGITUDE], 8) AS [LONGITUDE]
    ,ROUND([LATITUDE], 8) AS [LATITUDE]
    ,ROUND([LENGTH], 4) AS [LENGTH]
    ,ROUND([AREA], 4) AS [AREA]
    ,CAST([GEOMTYPE] AS VARCHAR(30)) AS [GEOMTYPE]
    ,CAST([COUNCIL2021] AS VARCHAR(1020)) AS [COUNCIL2021]
    ,CAST([COUNCIL2021_DOMINANT] AS VARCHAR(255)) AS [COUNCIL2021_DOMINANT]
    ,CAST([COUNCIL2021_AREAS] AS VARCHAR(MAX)) AS [COUNCIL2021_AREAS]
    ,CAST([LEGISLATIVE2022] AS VARCHAR(1020)) AS [LEGISLATIVE2022]
    ,CAST([LEGISLATIVE2022_DOMINANT] AS VARCHAR(255)) AS [LEGISLATIVE2022_DOMINANT]
    ,CAST([LEGISLATIVE2022_AREAS] AS VARCHAR(MAX)) AS [LEGISLATIVE2022_AREAS]
    ,CAST([CONGRESS2021] AS VARCHAR(1020)) AS [CONGRESS2021]
    ,CAST([CONGRESS2021_DOMINANT] AS VARCHAR(255)) AS [CONGRESS2021_DOMINANT]
    ,CAST([CONGRESS2021_AREAS] AS VARCHAR(MAX)) AS [CONGRESS2021_AREAS]
    ,CAST([PARKPOLICEBEAT] AS VARCHAR(1020)) AS [PARKPOLICEBEAT]
    ,CAST([PARKPOLICEBEAT_DOMINANT] AS VARCHAR(255)) AS [PARKPOLICEBEAT_DOMINANT]
    ,CAST([PARKPOLICEBEAT_AREAS] AS VARCHAR(MAX)) AS [PARKPOLICEBEAT_AREAS]
    ,CAST([REGIONALSERVICECENTER] AS VARCHAR(1020)) AS [REGIONALSERVICECENTER]
    ,CAST([REGIONALSERVICECENTER_DOMINANT] AS VARCHAR(255)) AS [REGIONALSERVICECENTER_DOMINANT]
    ,CAST([REGIONALSERVICECENTER_AREAS] AS VARCHAR(MAX)) AS [REGIONALSERVICECENTER_AREAS]
    ,CAST([CENSUSTRACT2020] AS VARCHAR(1020)) AS [CENSUSTRACT2020]
    ,CAST([CENSUSTRACT2020_DOMINANT] AS VARCHAR(255)) AS [CENSUSTRACT2020_DOMINANT]
    ,CAST([CENSUSTRACT2020_AREAS] AS VARCHAR(MAX)) AS [CENSUSTRACT2020_AREAS]
    ,CAST([CENSUSTRACT2010] AS VARCHAR(1020)) AS [CENSUSTRACT2010]
    ,CAST([CENSUSTRACT2010_DOMINANT] AS VARCHAR(255)) AS [CENSUSTRACT2010_DOMINANT]
    ,CAST([CENSUSTRACT2010_AREAS] AS VARCHAR(MAX)) AS [CENSUSTRACT2010_AREAS]
    ,CAST([GEOM_PARK_CODE] AS VARCHAR(150)) AS [GEOM_PARK_CODE]
    ,CAST([GEOM_PARK_CODE_DOMINANT] AS VARCHAR(30)) AS [GEOM_PARK_CODE_DOMINANT]
    ,CAST([GEOM_PARK_CODE_AREAS] AS VARCHAR(MAX)) AS [GEOM_PARK_CODE_AREAS]
    ,CAST([GEOM_PARK_CODE_NEAREST] AS VARCHAR(30)) AS [GEOM_PARK_CODE_NEAREST]
    ,ROUND([GEOM_PARK_CODE_NEARESTDISTANCE], 4) AS [GEOM_PARK_CODE_NEARESTDISTANCE]
    ,CAST([GEOM_PARK_CODE_NEARESTAREAS] AS VARCHAR(MAX)) AS [GEOM_PARK_CODE_NEARESTAREAS]'''

    return sql

def get_featurelayer_silversqlprocedure(
    featurelayer_url : str,
    gis : GIS,
    name : str
) -> str:
    name_lower = name.lower()
    name_upper = name.upper()

    field_selection = get_featurelayer_silversqlfields(featurelayer_url=featurelayer_url, gis=gis)
    procedure = f"""
USE mds_ldw;
GO
IF OBJECT_ID('dbo.USP_S_GIS_{name_upper}') IS NOT NULL
    DROP PROCEDURE [dbo].[USP_S_GIS_{name_upper}]
GO

CREATE PROCEDURE [dbo].[USP_S_GIS_{name_upper}]
    @external_tbl_schema NVARCHAR(128),
    @external_location VARCHAR(128),
    @external_tbl_name NVARCHAR(128)
AS
BEGIN
    DECLARE @drop_existing_sql NVARCHAR(500)='IF OBJECT_ID('''+@external_tbl_schema +'.'+@external_tbl_name+''') IS NOT NULL DROP EXTERNAL TABLE ['+@external_tbl_schema +'].['+@external_tbl_name+']'
    DECLARE @external_data_source sysname='mds_ldw_source';
    DECLARE @external_file_format sysname='raw_ion_parquet';

    PRINT @drop_existing_sql
    EXECUTE sp_executesql @drop_existing_sql
    PRINT 'DROPPED EXTERNAL TABLE: '+@external_tbl_name

CREATE EXTERNAL TABLE [silver].[S_GIS_{name_upper}]
    WITH (
        LOCATION = 'silver/gis-{name_lower}',
        DATA_SOURCE = mds_ldw_source,
        FILE_FORMAT = raw_ion_parquet
    ) AS
SELECT
{field_selection}
FROM (
    SELECT
    *
    ,ROW_NUMBER() OVER (PARTITION BY [OBJECTID] ORDER BY [INGEST_TS] DESC) AS [row_num]
    ,RANK() OVER (ORDER BY [INGEST_TS] DESC) AS [ingest_num]
    FROM [bronze].[B_GIS_{name_upper}]
)t1
WHERE
    [row_num] = 1
    AND [ingest_num] = 1;
END;

/*
EXECUTE dbo.USP_S_GIS_{name_upper}
    @external_tbl_schema='silver',
    @external_location='silver/gis-{name_lower}',
    @external_tbl_name='S_GIS_{name_upper}';
SELECT TOP(10) * FROM [silver].[S_GIS_{name_upper}];
*/

"""
    return procedure

def get_prefixed_field_aliases(
    sql : str,
    prefix : str
) -> str:
    new_sql = ''
    for line in sql.split('\n')[1:-1]:
        new_line = line.replace('[', f'[{prefix.lower()}].[')
        field_name = line.split('[')[1].split(']')[0]
        new_sql += new_line + f' AS [{prefix}_{field_name}]\n'

    return new_sql

def print_geolookup_fields(
    fields: list[str]
) -> str:
    bronze = ''
    for field in fields:
        bronze += f"""
{field} VARCHAR(8000),
{field}_DOMINANT VARCHAR(8000),
{field}_AREAS VARCHAR(8000),"""
    bronze = bronze[:-1]

    silver = ''
    for field in fields:
        silver += f"""
CAST([{field}] AS VARCHAR(1020)) AS [{field}],
CAST({field}_DOMINANT AS VARCHAR(255)) AS {field}_DOMINANT,
CAST({field}_AREAS AS VARCHAR(MAX)) AS {field}_AREAS,"""
    silver = silver[:-1]
    print(bronze)
    print("""

-------------------

""")
    print(silver)

# In[50]:

# print_geolookup_fields(['COUNCIL2021', 'LEGISLATIVE2022', 'CONGRESS2021', 'PARKPOLICEBEAT', 'REGIONALSERVICECENTER', 'CENSUSTRACT2020', 'CENSUSTRACT2010'])
# In[50]:

layers = [
    # ('Benches', 'https://services1.arcgis.com/HbzrdBZjOwNHp70P/arcgis/rest/services/Montgomery_Parks_Benches/FeatureServer/0'), # 19ce5bf022dc4193aeac8b18833e094f
    # ('BikeAssets', 'https://services1.arcgis.com/HbzrdBZjOwNHp70P/arcgis/rest/services/Bike_Assets/FeatureServer/0'), # 46de90ff5980452b9beffe37d377252a
    # ('Kiosks', 'https://services1.arcgis.com/HbzrdBZjOwNHp70P/arcgis/rest/services/Kiosks_and_Signs/FeatureServer/1'), # d4a8d962a98a4944bfd665fe8bea2fcb
    # ('SignLocations', 'https://services1.arcgis.com/HbzrdBZjOwNHp70P/arcgis/rest/services/Kiosks_and_Signs/FeatureServer/0'), # d4a8d962a98a4944bfd665fe8bea2fcb
    # ('AssetsOther', 'https://montgomeryplans.org/server/rest/services/Parks/Assets_Pt/FeatureServer/0'),
    # ('Courts', 'https://montgomeryplans.org/server/rest/services/Courts/Courts/FeatureServer/0'), # 151a6f063c2a468fa927c2150d909da1
    # ('CourtPads', 'https://montgomeryplans.org/server/rest/services/Courts/Courts/FeatureServer/1'), # 151a6f063c2a468fa927c2150d909da1
    # ('PortaJohnLocations', 'https://services1.arcgis.com/HbzrdBZjOwNHp70P/arcgis/rest/services/Portajohn_Locations/FeatureServer/1'), # e2d98f9697a84f94b41f3455b9db38a5
    # ('PicnicShelters', 'https://services1.arcgis.com/HbzrdBZjOwNHp70P/arcgis/rest/services/PicnicShelters/FeatureServer/0'), # a067549da0e44ad59fe4e5999cca3304
    # ('TreeInventory', 'https://montgomeryplans.org/server/rest/services/Arboriculture/TreeInventory_Pt/FeatureServer/0'), # e2d98f9697a84f94b41f3455b9db38a5
    # ('TreeSpecies', 'https://services1.arcgis.com/HbzrdBZjOwNHp70P/arcgis/rest/services/Tree_Species_List/FeatureServer/0'), # 2be8dd3aa4df498e8213138fe0c06168
    # ('AthleticFields', 'https://services1.arcgis.com/HbzrdBZjOwNHp70P/arcgis/rest/services/AthleticFields/FeatureServer/0'), # 87ccb6bc975e41178ae4f05ff6834324
    # ('CensusTracts2020', 'https://tigerweb.geo.census.gov/arcgis/rest/services/Census2020/tigerWMS_Census2020/MapServer/6'),
    # ('ParkUnits', 'https://montgomeryplans.org/server/rest/services/Parks/ParkUnits_Py/FeatureServer/0'), # 727bab07c5da4c81b88cabdf16a5cf44
    # ('CensusTracts2010', 'https://tigerweb.geo.census.gov/arcgis/rest/services/TIGERweb/tigerWMS_Census2010/MapServer/14'),
    # ('Bleachers', 'https://services1.arcgis.com/HbzrdBZjOwNHp70P/arcgis/rest/services/Bleachers/FeatureServer/0'), # fb8f7d0b1a4c4ef79d7b8d0987364844
    # ('Playgrounds', 'https://services1.arcgis.com/HbzrdBZjOwNHp70P/arcgis/rest/services/Playgrounds_Editable/FeatureServer/0'), # 524972064e324e70a61dfdbfefe875c6
    # ('CommunityGardens', 'https://services1.arcgis.com/HbzrdBZjOwNHp70P/arcgis/rest/services/Montgomery_Parks_Community_Gardens/FeatureServer/1'), # ab1834b34cbd47369833c8131aa58d09
    # ('ElectricTelecomPoints', 'https://services1.arcgis.com/HbzrdBZjOwNHp70P/arcgis/rest/services/Park_Utilities/FeatureServer/0'), # e43c4107390e4211a862469ded34bb1e
    # ('DogParks', 'https://services1.arcgis.com/HbzrdBZjOwNHp70P/arcgis/rest/services/Dog_Parks/FeatureServer/0'), # 0044b8efb8184c1caaeb41820eb5261f
    # ('BikeSkateParks', 'https://services1.arcgis.com/HbzrdBZjOwNHp70P/arcgis/rest/services/Skate_Parks/FeatureServer/0'), # 9b81d15eccd541ba96073ba3de6175df
    # ('BCISurveySites', 'https://services1.arcgis.com/HbzrdBZjOwNHp70P/ArcGIS/rest/services/BCI_SurveyPoints/FeatureServer/0'), # 40f32a2db2c44223a13c4126f5441c1f
    # ('BCISurveyResults', 'https://services1.arcgis.com/HbzrdBZjOwNHp70P/ArcGIS/rest/services/BCI_SurveyPoints/FeatureServer/2'), # 40f32a2db2c44223a13c4126f5441c1f
    # ('BCISoundMonitorResults', 'https://services1.arcgis.com/HbzrdBZjOwNHp70P/ArcGIS/rest/services/BCI_SurveyPoints/FeatureServer/4'), # 40f32a2db2c44223a13c4126f5441c1f
    # ('BCISpecies', 'https://services1.arcgis.com/HbzrdBZjOwNHp70P/ArcGIS/rest/services/BCI_SurveyPoints/FeatureServer/3'), # 40f32a2db2c44223a13c4126f5441c1f
    # ('WWLocations', 'https://services1.arcgis.com/HbzrdBZjOwNHp70P/arcgis/rest/services/ac030a/FeatureServer/0'), # 8e0ca62908d94cedb5919f82eadf370c
    # ('TrailUnits', 'https://montgomeryplans.org/server/rest/services/Trails/TrailUnits_Ln_EDIT/FeatureServer/0'), # 538a3b30596a42b58aea4957e58d3af0
    # ('BridgesBoardwalksCulvertsDocks', 'https://utility.arcgis.com/usrsvcs/servers/b9b1473ecb3e4fe89d4b6fcd7d5a94bd/rest/services/EnvironmentalEngineering/ParkBridges_Pt_EDIT/FeatureServer/0'), # b9b1473ecb3e4fe89d4b6fcd7d5a94bd, url=https://montgomeryplans.org/server/rest/services/EnvironmentalEngineering/ParkBridges_Pt_EDIT/FeatureServer/0
    # ('TrailCounterSites', 'https://services1.arcgis.com/HbzrdBZjOwNHp70P/arcgis/rest/services/TrailCounters/FeatureServer/0'), # 0d3b5b42f46c417d87e08e9b0d1de41f
    # ('Meadows', 'https://services1.arcgis.com/HbzrdBZjOwNHp70P/arcgis/rest/services/Montgomery_Parks_Meadows/FeatureServer/0'), # 3a1e87352337435db98fdb2ebdda640e,
    # ('ReforestationSites', 'https://utility.arcgis.com/usrsvcs/servers/dc1665c1e5b746cd8804899b9cc33bc5/rest/services/NaturalResources/ReforestationSites_Py_EDIT/FeatureServer/0'), # dc1665c1e5b746cd8804899b9cc33bc5 url=https://montgomeryplans.org/server/rest/services/NaturalResources/ReforestationSites_Py_EDIT/FeatureServer/0
    # ('BioMonVisits', 'https://services1.arcgis.com/HbzrdBZjOwNHp70P/arcgis/rest/services/BioMon_EDIT/FeatureServer/0'), # e30e7a3a36cc4d47a22dc8daf83f2eeb,
    # ('BioMonShockers', 'https://services1.arcgis.com/HbzrdBZjOwNHp70P/arcgis/rest/services/BioMon_EDIT/FeatureServer/1'), # e30e7a3a36cc4d47a22dc8daf83f2eeb,
    # ('BioMonFishCounts', 'https://services1.arcgis.com/HbzrdBZjOwNHp70P/arcgis/rest/services/BioMon_EDIT/FeatureServer/2'), # e30e7a3a36cc4d47a22dc8daf83f2eeb,
    # ('BioMonGameFish', 'https://services1.arcgis.com/HbzrdBZjOwNHp70P/arcgis/rest/services/BioMon_EDIT/FeatureServer/3'), # e30e7a3a36cc4d47a22dc8daf83f2eeb,
    # ('BioMonPersonnel', 'https://services1.arcgis.com/HbzrdBZjOwNHp70P/arcgis/rest/services/BioMon_EDIT/FeatureServer/4'), # e30e7a3a36cc4d47a22dc8daf83f2eeb,
]

for layer in layers:
    print(layer[0])
    # print('')
    print(get_featurelayer_field_names(layer[1], gis=gis))
    print('')
    print(get_featurelayer_bronzesqlfields(layer[1], gis=gis, name=layer[0]))
    print('')
    print(get_featurelayer_silversqlprocedure(layer[1], gis=gis, name=layer[0]))
    print('')

# In[ ]:
# sql = ''''''
# for layer_name in [x[0] for x in layers]:
#     sql += f"""
# SELECT
# 'OBJECTID,
# STATUS,
# CLASS,
# CATEGORY,
# SUBCATEGORY,
# PARK_NAME,
# PARK_CODE,
# LOCATION_NAME,
# LOCATION_CODE,
# TRAIL_NAME,
# GISOBJID, MATERIAL, SIZE_, MGMT_AREA, MGMT_REGION, WITHDRAW, GLOBALID, DESCRIPTION, OWNER, MANAGER, ADA_COMPLIANT, ADA_ACCESSIBLE, HISTORIC, CLUSTER_ID, COMMISS, CREATED_DATE, UPDATED_DATE, CREATED_USER, UPDATED_USER, PARENT, LATEST_QAQC, ASSET'
# """

# In[ ]:
sql = '''
[LASTSAVED]
,[OBTYPE]
,[OBTYPEDESC]
,[OBRTYPE]
,[CODE]
,[DESC]
,[CLASS]
,[CLASSDESC]
,[CATEGORY]
,[CATEGORYDESC]
,[LOCATION]
,[PARENT]
,[MANUFACT]
,[MRC]
,[MRCDESC]
,[OWNER]
,[OWNERDESC]
,[SERIALNO]
,[STATUS]
,[STATUSDESC]
,[RSTATUS]
,[COMMISS]
,[WITHDRAW]
,[RECORD]
,[GROUP]
,[USER]
,[PRODUCTION]
,[PRIMARYUOM]
,[ACD]
,[NOTUSED]
,[MANUFACTMODEL]
,[VALUE]
,[RSTATE]
,[UPDATED]
,[UPDATEDBY]
,[PERSON]
,[UPDATECOUNT]
,[GISOBJID]
,[GISLAYER]
,[XLOCATION]
,[YLOCATION]
,[GEOREF]
,[FIXED_ASSETID]
,[SUBCATEGORY]
,[LOCATION_NAME]
,[ADA_COMPLIANT]
,[ASSET_TAG]
,[ASSET_COORDINATOR]
,[ASSET_TYPE]
,[PHYSICAL_LOCATION]
,[ACCESS_LOCATION]
,[ADA_ACCESSIBLE]
,[ADA_CANDIDATE]
,[HISTORIC]
,[HISTORIC_PERMIT]
,[HISTORIC_ARCHEASEMENT]
,[POTENTIAL_ARCHSITE]
,[REGION]
,[YEARBUILT]
,[LASTSTATUSUPDATE]
,[XCOORDINATE]
,[YCOORDINATE]
,[CREATED]
,[LATESTINSTALLDATE]
,[LATESTRECEIPTDATE]
,[PARKCOUNTED]
,[PESTICIDEFREE]
,[2040SERVICEAREA]
,[ACCOUNTUNITCIP]
,[ACTIVITYACCOUNTCIP]
,[ACCOUNTCODEPARTS]
,[ACCOUNTCODESERVICE]
,[ECAP_PK]
,[COUNCILMANICDISTRICT]
,[LEGISLATIVEDISTRICT]
,[FIELDDIMENSIONS]
,[RECTORDIAM]
,[OVERLAYTYPE]
,[INPUT_FILE]
,[LAST_UPDATE_TS]
'''

# print(get_prefixed_field_aliases(sql, 'EAM'))
# %%
sql = '''
,[OBJECTID]
,[PARK_NAME]
,[DESCRIPTION]
,[NUM_TREES]
,[TREES_PER_ACRE]
,[ACRES]
,[PLANT_DATE]
,[PLANTER_TYPE]
,[PLANTER_DESC]
,[PROJECT]
,[PROJECT_MANAGER]
,[EASEMENT]
,[PLANT_DETAILS]
,[SITE_OTHER]
,[WARRENTY_YEARS]
,[WARRENTY_END]
,[GISOBJID]
,[MGMT_AREA]
,[MGMT_REGION]
,[CREATIONDATE]
,[EDITDATE]
,[CREATOR]
,[EDITOR]
,[ASSET]
,[PARK_CODE]
,[STATUS]
,[LOCATION_NAME]
,[LOCATION_CODE]
,[OWNER]
,[MANAGER]
,[PARENT]
,[COMMISS]
,[WITHDRAW]
,[LATEST_QAQC]
,[TRAIL_NAME]
,[INGEST_TS]
,[INGEST_FILE]
,[GEOMWKB]
,[GEOMWKT]
,[X]
,[Y]
,[LONGITUDE]
,[LATITUDE]
,[LENGTH]
,[AREA]
,[GEOMTYPE]
'''

print(get_prefixed_field_aliases(sql, 'GIS'))
# %%
