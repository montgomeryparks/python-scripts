import arcpy


def get_portal_token():
    try:
        t = arcpy.GetSigninToken()
        if isinstance(t, dict):
            return t.get("token", "")
    except Exception:
        pass
    return ""
