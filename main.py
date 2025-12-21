from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pdf_parser import extract_records_from_bytes
import uvicorn

app = FastAPI(title="SiSU PDF Parser API")

# Configure CORS so your Next.js app can call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust this in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def read_root():
    return {"status": "ok", "message": "SiSU Parser API is running"}

@app.post("/parse")
async def parse_pdf(file: UploadFile = File(...)):
    if not file.filename.endswith(".pdf"):
        raise HTTPException(status_code=400, detail="File must be a PDF")
    
    try:
        content = await file.read()
        records = extract_records_from_bytes(content)
        return {"count": len(records), "data": records}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# For local development with: python api/main.py
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
