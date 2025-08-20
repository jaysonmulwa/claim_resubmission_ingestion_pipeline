from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
import os
import shutil
from pathlib import Path

app = FastAPI(title="Claim Resubmission API", version="1.0.0")

# Configuration
UPLOAD_DIR = "uploads"
OUTPUTS_DIR = "outputs"
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB
ALLOWED_EXTENSIONS = {".json", ".csv"}

# Create upload directory if it doesn't exist
Path(UPLOAD_DIR).mkdir(exist_ok=True)

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    """
    Upload a file to the server
    
    Args:
        file: The file to upload
        
    Returns:
        JSON response with upload status and file info
    """
    
    # Check file size
    if file.size > MAX_FILE_SIZE:
        raise HTTPException(
            status_code=413,
            detail=f"File too large. Maximum size is {MAX_FILE_SIZE / (1024*1024):.1f}MB"
        )
    
    # Check file extension
    file_extension = Path(file.filename).suffix.lower()
    if file_extension not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"File type not allowed. Allowed types: {', '.join(ALLOWED_EXTENSIONS)}"
        )
    
    try:
        # Generate safe filename
        safe_filename = f"{file.filename}"
        file_path = Path(UPLOAD_DIR) / safe_filename
        
        # Handle filename conflicts
        counter = 1
        while file_path.exists():
            name = Path(file.filename).stem
            ext = Path(file.filename).suffix
            safe_filename = f"{name}_{counter}{ext}"
            file_path = Path(UPLOAD_DIR) / safe_filename
            counter += 1
        
        # Save file
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        return JSONResponse(
            status_code=200,
            content={
                "message": "File uploaded successfully",
                "filename": safe_filename,
                "original_filename": file.filename,
                "file_size": file.size,
                "content_type": file.content_type,
                "file_path": str(file_path)
            }
        )
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to upload file: {str(e)}"
        )

@app.get("/uploads")
async def list_files():
    """
    List all uploaded files
    """
    try:
        files = []
        upload_path = Path(UPLOAD_DIR)
        
        if upload_path.exists():
            for file_path in upload_path.iterdir():
                if file_path.is_file():
                    stat = file_path.stat()
                    files.append({
                        "filename": file_path.name,
                        "size": stat.st_size,
                        "modified": stat.st_mtime
                    })
        
        return {"files": files}
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list files: {str(e)}"
        )
    
@app.get("/outputs")
async def list_outputs():
    """
    List all files output from the pipeline
    """
    try:
        files = []
        upload_path = Path(OUTPUTS_DIR)
        
        if upload_path.exists():
            for file_path in upload_path.iterdir():
                if file_path.is_file():
                    stat = file_path.stat()
                    files.append({
                        "filename": file_path.name,
                        "size": stat.st_size,
                        "modified": stat.st_mtime
                    })
        
        return {"files": files}
        
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to list files: {str(e)}"
        )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)