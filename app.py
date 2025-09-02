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
    return manager.get_antisemitic_with_weapons()




if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
