'''
Author: Carter Hughes
Date: 20260306
Purpose: Help draft parameters, T-SQL, etc. for querying data originating from a Feature or Map Service in Azure
'''

# In[1]:


from arcgis.gis import GIS
from arcgis.features import FeatureLayer
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
            sql += field['name'] + ' ' + type_mappings[field['type']](field) + ''',
'''
    sql = sql[:-2]
    return sql


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
    print(get_featurelayer_bronzesqlfields(layer[1], gis=gis))
    print('')