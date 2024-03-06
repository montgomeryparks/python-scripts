'''
Author: Carter Hughes
Date: 20260306
Purpose: Help draft parameters, T-SQL, etc. for querying data originating from a Feature or Map Service in Azure
'''

# In[1]:


from arcgis.gis import GIS
from arcgis.features import FeatureLayer
gis = GIS("home")


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
    return ', '.join([field['name'] for field in get_featurelayer_fields(featurelayer_url=featurelayer_url, gis=gis)])

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
        'esriFieldTypeBlob': lambda field: 'BINARY'
    }
    sql = ''''''
    for field in fields:
        sql += field['name'] + ' ' + type_mappings[field['type']](field) + ''',
'''
    sql = sql[:-2]
    return sql


# In[50]:


print(get_featurelayer_bronzesqlfields('https://services1.arcgis.com/HbzrdBZjOwNHp70P/arcgis/rest/services/ParkBuildings/FeatureServer/0', gis=gis))