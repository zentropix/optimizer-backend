from app.router import router
from fastapi import FastAPI
import uvicorn

app = FastAPI()
app.include_router(router)

@app.get("/")
def read_root():
    return {"message": "Optimizer Backend is running!"}

@app.get("/health")
def health():
    return {"status": "ok"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8800)
