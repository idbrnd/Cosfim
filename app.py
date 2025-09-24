from fastapi import FastAPI
from main import get_data
from dto import CosfimData
import uvicorn

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Hello, World!"}



@app.get("/cosfim/get-data")
async def get_data(cosfim_data: CosfimData):
    get_data(cosfim_data)
    return {"message": "success"}

    
if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=8000, factory=True)   
    # uvicorn.run("app:app", host="0.0.0.0", port=8000, factory=True, workers=4)   
