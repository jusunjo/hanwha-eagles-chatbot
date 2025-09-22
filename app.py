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

@app.post("")
async def kakao_webhook(request: Request):
    """
    Kakao chatbot webhook endpoint (기존 kakao_service 사용)
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

@app.post("/kakao")
async def kakao_rag_webhook(request: Request):
    """
    Kakao chatbot webhook endpoint with RAG system
    """
    try:
        # 요청 데이터 파싱
        request_data = await request.json()
        logger.info(f"Received Kakao RAG request: {json.dumps(request_data, ensure_ascii=False, indent=2)}")
        
        # 사용자 메시지 추출
        user_message = ""
        if "userRequest" in request_data and "utterance" in request_data["userRequest"]:
            user_message = request_data["userRequest"]["utterance"]
        
        if not user_message:
            return JSONResponse(content={
                "version": "2.0",
                "template": {
                    "outputs": [{
                        "simpleText": {
                            "text": "메시지를 입력해주세요."
                        }
                    }]
                }
            })
        
        # RAG 기반 Text-to-SQL 호출
        from rag.rag_text_to_sql import RAGTextToSQL
        rag_text_to_sql = RAGTextToSQL()
        answer = rag_text_to_sql.process_question(user_message)
        
        # 카카오 응답 형식으로 변환
        response = {
            "version": "2.0",
            "template": {
                "outputs": [{
                    "simpleText": {
                        "text": answer
                    }
                }]
            }
        }
        
        logger.info(f"Kakao RAG response: {json.dumps(response, ensure_ascii=False, indent=2)}")
        return JSONResponse(content=response)
        
    except Exception as e:
        logger.error(f"Error processing Kakao RAG request: {str(e)}")
        return JSONResponse(content={
            "version": "2.0",
            "template": {
                "outputs": [{
                    "simpleText": {
                        "text": f"오류가 발생했습니다: {str(e)}"
                    }
                }]
            }
        })

@app.post("/test")
async def test_endpoint(request: Request):
    """
    Test endpoint for direct text-to-sql testing (기존 하드코딩된 시스템)
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

@app.post("/rag-test")
async def rag_test_endpoint(request: Request):
    """
    RAG 기반 Text-to-SQL 테스트 엔드포인트 (동적 스키마 제공)
    """
    try:
        request_data = await request.json()
        question = request_data.get("message", "")
        
        if not question:
            return JSONResponse(content={"error": "No message provided"}, status_code=400)
        
        # RAG 기반 Text-to-SQL 호출
        from rag.rag_text_to_sql import RAGTextToSQL
        rag_text_to_sql = RAGTextToSQL()
        answer = rag_text_to_sql.process_question(question)
        
        return JSONResponse(content={"answer": answer})
        
    except Exception as e:
        logger.error(f"Error in rag-test endpoint: {str(e)}")
        return JSONResponse(content={"error": str(e)}, status_code=500)

@app.get("/schema")
async def get_schema_info():
    """
    스키마 정보 조회 엔드포인트
    """
    try:
        from rag.schema_manager import SchemaManager
        schema_manager = SchemaManager()
        
        return JSONResponse(content={
            "schema": schema_manager.schema_info,
            "message": "스키마 정보를 성공적으로 조회했습니다."
        })
        
    except Exception as e:
        logger.error(f"Error getting schema info: {str(e)}")
        return JSONResponse(content={"error": str(e)}, status_code=500)


if __name__ == "__main__":
    import uvicorn
    import os
    
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)