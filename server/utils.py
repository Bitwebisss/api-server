# server/utils.py

def ok(result, rid="api-server"):
    return {"error": None, "id": rid, "result": result}

def err(code: int, message: str, rid="api-server"):
    return {"error": {"code": code, "message": message}, "id": rid, "result": None}
