'''
Author: Carter Hughes
Date: 20260306
Purpose: Help draft parameters, T-SQL, etc. for querying data originating from a Feature or Map Service in Azure
'''

# In[1]:


from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from itertools import reduce
gis = GIS(profile="work")


# In[48]:
IGNORED_FIELDS = [
    'X',
    'Y',
    'ASSET_TYPE'
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
    gis : GIS
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
    sql = ''''''
    for field in fields:
        if field['name'] not in IGNORED_FIELDS:
            sql += field['name'] + ' VARCHAR(8000)' + ''',
'''
    sql = sql[:-2] + ''',
GEOMWKB VARBINARY(MAX),
GEOMWKT VARCHAR(MAX),
X FLOAT,
Y FLOAT,
LONGITUDE FLOAT,
LATITUDE FLOAT,
LENGTH VARCHAR(8000),
AREA VARCHAR(8000),
GEOMTYPE VARCHAR(8000)
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
    clean_name = name.upper()

    print(name)
    print(clean_name)
    return clean_name

    
def get_featurelayer_silversqlfields(
    featurelayer_url : str,
    gis : GIS
) -> str:
    fields = get_featurelayer_fields(featurelayer_url=featurelayer_url, gis=gis)
    type_mappings = {
        'esriFieldTypeString': lambda field: f"CAST([{field['name']}] AS VARCHAR({field['length']})) AS [{clean_field_name(field['name'])}]",
        'esriFieldTypeSmallInteger': lambda field: f"CAST([{field['name']}] AS INT) AS [{clean_field_name(field['name'])}]",
        'esriFieldTypeBigInteger': lambda field: f"CAST([{field['name']}] AS BIGINT) AS [{clean_field_name(field['name'])}]",
        'esriFieldTypeInteger': lambda field: f"CAST([{field['name']}] AS INT) AS [{clean_field_name(field['name'])}]",
        'esriFieldTypeOID': lambda field: f"CAST([{field['name']}] AS INT) AS [{clean_field_name(field['name'])}]",
        'esriFieldTypeGlobalID': lambda field: f"CAST([{field['name']}] AS CHAR(36)) AS [{clean_field_name(field['name'])}]",
        'esriFieldTypeGUID': lambda field: f"CAST([{field['name']}] AS CHAR(36)) AS [{clean_field_name(field['name'])}]",
        'esriFieldTypeDate': lambda field: f"""DATEADD(S, [{field['name']}]/1000, ''1970-01-01'') AT TIME ZONE ''UTC'' AT TIME ZONE ''Eastern Standard Time'' AS [{clean_field_name(field['name'])}]""",
        'esriFieldTypeDouble' : lambda field: f"CAST([{field['name']}] AS NUMERIC(38, 8)) AS [{clean_field_name(field['name'])}]",
        'esriFieldTypeFloat' : lambda field: f"CAST([{field['name']}] AS NUMERIC(12, 6)) AS [{clean_field_name(field['name'])}]",
        'esriFieldTypeSingle' : lambda field: f"CAST([{field['name']}] AS NUMERIC(12, 6)) AS [{clean_field_name(field['name'])}]",
        'esriFieldTypeBlob': lambda field: f"CAST([{field['name']}] AS BINARY) AS [{clean_field_name(field['name'])}]"
    }
    sql = ''''''
    for field in fields:
        if field['name'] not in IGNORED_FIELDS:
            sql += '    ' + type_mappings[field['type']](field) + ''',
'''
    sql = sql[:-2] + ''',
    [GEOMWKB],
    [GEOMWKT],
    ROUND([X], 4) AS [X],
    ROUND([Y], 4) AS [Y],
    ROUND([LONGITUDE], 4) AS [LONGITUDE],
    ROUND([LATITUDE], 4) AS [LATITUDE],
    ROUND([LENGTH], 4) AS [LENGTH],
    ROUND([AREA], 4) AS AREA,
    CAST([GEOMTYPE] AS VARCHAR(30)) AS [GEOMTYPE]'''

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
    DECLARE @sql NVARCHAR(MAX)='CREATE EXTERNAL TABLE '+ @external_tbl_schema +'.'+ @external_tbl_name +' 
    WITH (LOCATION ='''+@external_location + ''', 
    DATA_SOURCE ='+ @external_data_source + ', 
    FILE_FORMAT ='+ @external_file_format +') AS 
select
{field_selection}
from (select *, row_number() over(partition by OBJECTID order by UPDATED_DATE desc) as row_num from bronze.B_GIS_{name_upper})t1
where row_num=1'

   PRINT @sql

   PRINT @drop_existing_sql
   
   EXECUTE sp_executesql @drop_existing_sql
   PRINT 'DROPPED EXTERNAL TABLE: '+@external_tbl_name
   EXECUTE sp_executesql @sql
   PRINT 'CREATED EXTERNAL TABLE: '+@external_tbl_name
END;

/*
EXECUTE dbo.USP_S_GIS_{name_upper}
    @external_tbl_schema='silver',
    @external_location='silver/gis-{name_lower}',
    @external_tbl_name='S_GIS_{name_upper}';
*/

"""
    return procedure
# In[50]:

layers = [
    ('Benches', 'https://services1.arcgis.com/HbzrdBZjOwNHp70P/arcgis/rest/services/Montgomery_Parks_Benches/FeatureServer/0'), # 19ce5bf022dc4193aeac8b18833e094f
    ('BikeAssets', 'https://services1.arcgis.com/HbzrdBZjOwNHp70P/arcgis/rest/services/Bike_Assets/FeatureServer/0'), # 46de90ff5980452b9beffe37d377252a
    ('Kiosks', 'https://services1.arcgis.com/HbzrdBZjOwNHp70P/arcgis/rest/services/Kiosks_and_Signs/FeatureServer/1'), # d4a8d962a98a4944bfd665fe8bea2fcb
    ('SignLocations', 'https://services1.arcgis.com/HbzrdBZjOwNHp70P/arcgis/rest/services/Kiosks_and_Signs/FeatureServer/0'), # d4a8d962a98a4944bfd665fe8bea2fcb
    ('AssetsOther', 'https://montgomeryplans.org/server/rest/services/Parks/Assets_Pt/FeatureServer/0')
]

for layer in layers:
    print(layer[0])
    print('')
    print(get_featurelayer_field_names(layer[1], gis=gis))
    print('')
    print(get_featurelayer_silversqlprocedure(layer[1], gis=gis, name=layer[0]))
    print('')