import uvicorn
from fastapi import FastAPI
from elasticsearch import Elasticsearch

from v2.manager import Manager

app = FastAPI()
manager=Manager()
manager.run()


@app.get("/antisemitic_with_weapons")
def get_antisemitic_with_weapons():
    if not manager.finish_processing:
        return {"message": "Processing not finished yet. Please try again later."}

    result = manager.get_antisemitic_with_weapons()
    if isinstance(result, dict) and "error" in result:
        return {"status": "error", "details": result["error"]}
    return result


@app.get("/weapons_more_two")
def get_weapons_more_two():
    if not manager.finish_processing:
        return {"message": "Processing not finished yet. Please try again later."}

    result = manager.get_weapons_more_two()
    if isinstance(result, dict) and "error" in result:
        return {"status": "error", "details": result["error"]}
    return result





#
# if __name__ == "__main__":
#     uvicorn.run(app, host="0.0.0.0", port=8000)
