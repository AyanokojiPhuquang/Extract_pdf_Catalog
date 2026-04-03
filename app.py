import fitz
import base64
import requests
import os
import uuid
import csv
import io
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, UploadFile, File, Request
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.templating import Jinja2Templates
from starlette.responses import StreamingResponse

from cost import calculate_batch_cost, accumulate_cost, SessionCost
from db import init_db, get_history, get_record, save_record, ExtractionRecord
from models import ModelCache

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("extractor")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize database on startup."""
    init_db()
    log.info("Database initialized")
    yield


app = FastAPI(lifespan=lifespan)

UPLOAD_DIR = Path(__file__).parent / "uploads"
UPLOAD_DIR.mkdir(exist_ok=True)

templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
MODEL = "anthropic/claude-sonnet-4"

model_cache = ModelCache()
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


def call_llm(images_b64, model=MODEL) -> tuple[str, dict]:
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

    log.info(f"Calling {model} with {len(images_b64)} images...")
    resp = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers={
            "Authorization": f"Bearer {API_KEY}",
            "Content-Type": "application/json",
        },
        json={
            "model": model,
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
    usage = data["choices"][0].get("usage", data.get("usage", {}))
    log.info(f"LLM response received ({len(content_text)} chars)")
    return content_text, usage


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


@app.get("/models")
async def list_models():
    """Return list of vision-capable AI models from OpenRouter API."""
    if not API_KEY:
        return JSONResponse({"error": "OPENROUTER_API_KEY not set"}, status_code=500)
    try:
        models = model_cache.get_models(API_KEY)
        return JSONResponse([
            {
                "id": m.id,
                "name": m.name,
                "prompt_price": m.prompt_price,
                "completion_price": m.completion_price,
            }
            for m in models
        ])
    except requests.RequestException as e:
        log.error(f"Failed to fetch models from OpenRouter API: {e}")
        return JSONResponse(
            {"error": f"OpenRouter API unavailable: {str(e)}"},
            status_code=502,
        )


@app.get("/extract")
async def extract(request: Request):
    file_id = request.query_params.get("file_id")
    start_page = int(request.query_params.get("start_page", 0))
    end_page = request.query_params.get("end_page")
    model = request.query_params.get("model") or MODEL
    filename = request.query_params.get("filename", "")

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

    # Get pricing info for cost calculation
    pricing = model_cache.get_model_pricing(model)
    prompt_price = pricing.prompt_price if pricing else 0.0
    completion_price = pricing.completion_price if pricing else 0.0

    def event_stream():
        total_batches = (end_page - start_page + BATCH_SIZE - 1) // BATCH_SIZE
        log.info(f"Starting extraction: pages {start_page+1}-{end_page}, {total_batches} batches, model={model}")
        session_cost = SessionCost(0, 0, 0.0)
        all_rows = []
        all_headers = []

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
                raw_csv, usage = call_llm(images, model=model)
                cleaned = clean_csv(raw_csv)
                headers, rows = parse_csv_text(cleaned)

                # Track headers from first batch
                if not all_headers and headers:
                    all_headers = headers
                all_rows.extend(rows)

                # Calculate batch cost
                cost_info = None
                if usage:
                    batch_cost = calculate_batch_cost(usage, prompt_price, completion_price)
                    if batch_cost:
                        cost_info = {
                            "prompt_tokens": batch_cost.prompt_tokens,
                            "completion_tokens": batch_cost.completion_tokens,
                            "cost_usd": batch_cost.cost_usd,
                        }
                        session_cost = accumulate_cost(session_cost, batch_cost)
                    else:
                        log.warning(f"Batch {batch_idx+1}: usage info missing required keys")
                else:
                    log.warning(f"Batch {batch_idx+1}: no usage info in API response")

                batch_data = {
                    "type": "batch",
                    "batch": batch_idx + 1,
                    "headers": headers,
                    "rows": rows,
                    "pages": f"{batch_start + 1}-{batch_end}",
                    "cost": cost_info,
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

        # Build CSV data for DB record
        csv_output = io.StringIO()
        writer = csv.writer(csv_output)
        if all_headers:
            writer.writerow(all_headers)
        for row in all_rows:
            writer.writerow(row)
        csv_data = csv_output.getvalue()

        # Save record to DB
        record_id = None
        try:
            record = ExtractionRecord(
                id=None,
                file_id=file_id,
                filename=filename or f"{file_id}.pdf",
                model_name=model,
                start_page=start_page,
                end_page=end_page,
                product_count=len(all_rows),
                csv_data=csv_data,
                total_cost=session_cost.total_cost_usd if session_cost.total_cost_usd > 0 else None,
                prompt_tokens=session_cost.total_prompt_tokens if session_cost.total_prompt_tokens > 0 else None,
                completion_tokens=session_cost.total_completion_tokens if session_cost.total_completion_tokens > 0 else None,
                created_at=datetime.now().isoformat(sep=" ", timespec="seconds"),
            )
            record_id = save_record(record)
            log.info(f"Saved extraction record #{record_id}")
        except Exception as e:
            log.error(f"Failed to save extraction record to DB: {e}")

        done_data = {
            "type": "done",
            "record_id": record_id,
            "total_cost": {
                "prompt_tokens": session_cost.total_prompt_tokens,
                "completion_tokens": session_cost.total_completion_tokens,
                "total_cost_usd": session_cost.total_cost_usd,
            },
        }
        log.info("Extraction complete!")
        yield f"data: {json.dumps(done_data)}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "X-Accel-Buffering": "no",
            "Cache-Control": "no-cache, no-transform",
            "Connection": "keep-alive",
        },
    )


@app.get("/history")
async def history(request: Request):
    """Return list of extraction history records (without csv_data)."""
    limit = int(request.query_params.get("limit", 50))
    offset = int(request.query_params.get("offset", 0))
    records = get_history(limit=limit, offset=offset)
    return JSONResponse(records)


@app.get("/history/{record_id}")
async def history_detail(record_id: int):
    """Return full details of a single extraction record."""
    record = get_record(record_id)
    if record is None:
        return JSONResponse({"error": "Record not found"}, status_code=404)
    from dataclasses import asdict
    return JSONResponse(asdict(record))


@app.get("/history/{record_id}/csv")
async def history_csv(record_id: int):
    """Download CSV data from a history record."""
    record = get_record(record_id)
    if record is None:
        return JSONResponse({"error": "Record not found"}, status_code=404)

    def iter_csv():
        yield record.csv_data

    filename = f"{record.filename.rsplit('.', 1)[0]}_extracted.csv" if record.filename else "extracted.csv"
    return StreamingResponse(
        iter_csv(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
