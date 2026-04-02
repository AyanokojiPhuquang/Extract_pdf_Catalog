import fitz
import base64
import requests
import os
import uuid
import csv
import io
import json
import logging
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, UploadFile, File, Request
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.templating import Jinja2Templates
from starlette.responses import StreamingResponse

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("extractor")

app = FastAPI()

UPLOAD_DIR = Path(__file__).parent / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
MODEL = "anthropic/claude-sonnet-4"
BATCH_SIZE = 10


def pdf_pages_to_base64(pdf_path, start=0, end=None):
    doc = fitz.open(pdf_path)
    if end is None:
        end = len(doc)
    images = []
    for i in range(start, min(end, len(doc))):
        page = doc[i]
        pix = page.get_pixmap(dpi=120)
        img_bytes = pix.tobytes("png")
        images.append(base64.b64encode(img_bytes).decode("utf-8"))
        log.info(f"  Page {i+1} converted ({len(img_bytes)//1024} KB)")
    doc.close()
    return images


def call_llm(images_b64):
    content = [
        {
            "type": "text",
            "text": """Bạn là chuyên gia trích xuất dữ liệu từ catalogue sản phẩm.
Hãy xem tất cả các trang catalogue dưới đây và trích xuất TOÀN BỘ sản phẩm thành bảng CSV với các cột:
- ma_san_pham: Mã sản phẩm (model number/code)
- ten_san_pham: Tên sản phẩm
- gia_niem_yet: Giá niêm yết
- gia_ban: Giá bán
- brand: Thương hiệu
- category: Danh mục sản phẩm

Quy tắc về giá:
- Nếu sản phẩm chỉ có 1 giá duy nhất → đó là gia_niem_yet, để trống gia_ban
- Nếu sản phẩm có 2 giá → giá CAO hơn là gia_niem_yet, giá THẤP hơn là gia_ban

Quy tắc chung:
- CHỈ trích xuất sản phẩm thực sự (có tên + giá). BỎ QUA các mục: thông tin tham khảo, bản vẽ kỹ thuật, thông số kỹ thuật, phụ kiện đi kèm miễn phí
- Trả về ĐÚNG format CSV, dùng dấu phẩy ngăn cách
- Dòng đầu tiên là header: ma_san_pham,ten_san_pham,gia_niem_yet,gia_ban,brand,category
- Nếu không có thông tin thì để trống
- Giá để nguyên số, không thêm ký tự
- Mỗi sản phẩm một dòng
- CHỈ trả về CSV, không giải thích gì thêm
- Nếu trang không chứa sản phẩm nào, chỉ trả về dòng header"""
        }
    ]
    for b64 in images_b64:
        content.append({
            "type": "image_url",
            "image_url": {"url": f"data:image/png;base64,{b64}"}
        })

    log.info(f"Calling {MODEL} with {len(images_b64)} images...")
    resp = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "model": MODEL,
            "messages": [{"role": "user", "content": content}],
            "max_tokens": 16000,
            "temperature": 0.1,
        },
        timeout=300,
    )
    if resp.status_code != 200:
        raise Exception(f"API Error {resp.status_code}: {resp.text[:500]}")
    try:
        data = resp.json()
    except Exception:
        raise Exception(f"Non-JSON response: {resp.text[:500]}")
    if "error" in data:
        raise Exception(f"API error: {data['error']}")
    content_text = data["choices"][0]["message"]["content"]
    log.info(f"LLM response received ({len(content_text)} chars)")
    return content_text


def clean_csv(raw):
    raw = raw.strip()
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1]
    if raw.endswith("```"):
        raw = raw.rsplit("```", 1)[0]
    return raw.strip()


def parse_csv_text(csv_text):
    reader = csv.reader(io.StringIO(csv_text))
    rows = list(reader)
    if not rows:
        return [], []
    return rows[0], rows[1:]


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")


@app.post("/upload")
async def upload_pdf(file: UploadFile = File(...)):
    file_id = str(uuid.uuid4())[:8]
    filepath = UPLOAD_DIR / f"{file_id}.pdf"
    content = await file.read()
    with open(filepath, "wb") as f:
        f.write(content)
    doc = fitz.open(str(filepath))
    total_pages = len(doc)
    doc.close()
    return JSONResponse({
        "file_id": file_id,
        "filename": file.filename,
        "total_pages": total_pages,
        "pdf_url": f"/pdf/{file_id}"
    })


@app.get("/pdf/{file_id}")
async def serve_pdf(file_id: str):
    filepath = UPLOAD_DIR / f"{file_id}.pdf"
    if not filepath.exists():
        return JSONResponse({"error": "File not found"}, status_code=404)
    return FileResponse(str(filepath), media_type="application/pdf")


@app.get("/extract")
async def extract(request: Request):
    file_id = request.query_params.get("file_id")
    start_page = int(request.query_params.get("start_page", 0))
    end_page = request.query_params.get("end_page")

    filepath = UPLOAD_DIR / f"{file_id}.pdf"
    if not filepath.exists():
        return JSONResponse({"error": "File not found"}, status_code=404)
    if not API_KEY:
        return JSONResponse({"error": "OPENROUTER_API_KEY not set"}, status_code=500)

    if end_page is None:
        doc = fitz.open(str(filepath))
        end_page = len(doc)
        doc.close()
    else:
        end_page = int(end_page)

    def event_stream():
        total_batches = (end_page - start_page + BATCH_SIZE - 1) // BATCH_SIZE
        log.info(f"Starting extraction: pages {start_page+1}-{end_page}, {total_batches} batches")
        for batch_idx, batch_start in enumerate(range(start_page, end_page, BATCH_SIZE)):
            batch_end = min(batch_start + BATCH_SIZE, end_page)
            log.info(f"--- Batch {batch_idx+1}/{total_batches}: pages {batch_start+1}-{batch_end} ---")
            # Send progress event
            progress = {
                "type": "progress",
                "batch": batch_idx + 1,
                "total_batches": total_batches,
                "pages": f"{batch_start + 1}-{batch_end}"
            }
            yield f"data: {json.dumps(progress)}\n\n"

            try:
                images = pdf_pages_to_base64(str(filepath), batch_start, batch_end)
                raw_csv = call_llm(images)
                cleaned = clean_csv(raw_csv)
                headers, rows = parse_csv_text(cleaned)

                batch_data = {
                    "type": "batch",
                    "batch": batch_idx + 1,
                    "headers": headers,
                    "rows": rows,
                    "pages": f"{batch_start + 1}-{batch_end}"
                }
                log.info(f"Batch {batch_idx+1}: extracted {len(rows)} products")
                yield f"data: {json.dumps(batch_data, ensure_ascii=False)}\n\n"
            except Exception as e:
                log.error(f"Batch {batch_idx+1} error: {e}")
                error_data = {
                    "type": "error",
                    "batch": batch_idx + 1,
                    "message": str(e),
                    "pages": f"{batch_start + 1}-{batch_end}"
                }
                yield f"data: {json.dumps(error_data, ensure_ascii=False)}\n\n"

        log.info("Extraction complete!")
        yield f"data: {json.dumps({'type': 'done'})}\n\n"

    return StreamingResponse(event_stream(), media_type="text/event-stream")
