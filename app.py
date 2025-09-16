"""
FastAPI application for Hanwha Eagles chatbot with Kakao integration.
"""

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
import json
import logging
from kakao_service import kakao_service

# 로깅 설정
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Hanwha Eagles Chatbot", version="1.0.0")

@app.get("/")
async def root():
    """Root endpoint"""
    return {"message": "Hanwha Eagles Chatbot API is running!"}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "hanwha-eagles-chatbot"}

@app.post("/kakao")
async def kakao_webhook(request: Request):
    """
    Kakao chatbot webhook endpoint
    """
    try:
        # 요청 데이터 파싱
        request_data = await request.json()
        logger.info(f"Received Kakao request: {json.dumps(request_data, ensure_ascii=False, indent=2)}")
        
        # 카카오 서비스를 통한 처리
        response = await kakao_service.process_kakao_request(request_data)
        
        logger.info(f"Kakao response: {json.dumps(response, ensure_ascii=False, indent=2)}")
        return JSONResponse(content=response)
        
    except Exception as e:
        logger.error(f"Error processing Kakao request: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.post("/test")
async def test_endpoint(request: Request):
    """
    Test endpoint for direct text-to-sql testing
    """
    try:
        request_data = await request.json()
        question = request_data.get("message", "")
        
        if not question:
            return JSONResponse(content={"error": "No message provided"}, status_code=400)
        
        # Text-to-SQL 직접 호출
        from rag.text_to_sql import TextToSQL
        text_to_sql = TextToSQL()
        answer = text_to_sql.process_question(question)
        
        return JSONResponse(content={"answer": answer})
        
    except Exception as e:
        logger.error(f"Error in test endpoint: {str(e)}")
        return JSONResponse(content={"error": str(e)}, status_code=500)

if __name__ == "__main__":
    import uvicorn
    import os
    
    # Railway에서 제공하는 PORT 환경변수 사용, 없으면 8000 사용
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)