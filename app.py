import fitz
import base64
import requests
import os
import uuid
import json
import logging
import urllib.parse
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, UploadFile, File, Request
from fastapi.responses import HTMLResponse, JSONResponse, FileResponse
from fastapi.templating import Jinja2Templates
from starlette.responses import StreamingResponse

from cost import calculate_batch_cost, accumulate_cost, SessionCost
from db import init_db, get_history, get_record, save_record, update_record_json, delete_record, ExtractionRecord
from excel_processor import read_excel_preview, classify_rows, build_descriptions_batch, build_output_rows, call_llm_text_batch, OUTPUT_HEADERS, RowType
from models import ModelCache

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("extractor")
HEADERS = ["ma_san_pham", "ma_phu_kien", "ten_san_pham", "hinh_anh", "mo_ta_tinh_nang", "kich_thuoc", "thuong_hieu", "danh_muc", "don_vi", "thue_vat", "gia_niem_yet", "gia_mua_vao", "gia_ban_ra"]

@asynccontextmanager
async def lifespan(app: FastAPI):
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


def call_llm(images_b64, model=MODEL):
    prompt_text = """Bạn là chuyên gia trích xuất dữ liệu từ catalogue sản phẩm.
Hãy xem tất cả các trang catalogue dưới đây và trích xuất TOÀN BỘ sản phẩm thành JSON array với các trường:
- "ma_san_pham": Mã sản phẩm (model number/code)
- "ma_phu_kien": Mã phụ kiện (nếu có)
- "ten_san_pham": Tên sản phẩm
- "hinh_anh": Để trống ""
- "mo_ta_tinh_nang": Mô tả và tính năng sản phẩm
- "kich_thuoc": Kích thước sản phẩm
- "thuong_hieu": Thương hiệu
- "danh_muc": Danh mục sản phẩm
- "don_vi": Để trống ""
- "thue_vat": Thuế VAT (nếu có)
- "gia_niem_yet": Giá niêm yết
- "gia_mua_vao": Để trống ""
- "gia_ban_ra": Giá bán ra

QUY TẮC QUAN TRỌNG VỀ NHIỀU MÃ SẢN PHẨM TRONG 1 HÌNH:
- Nếu 1 hình ảnh/mục hiển thị NHIỀU mã sản phẩm khác nhau thì PHẢI tách thành các sản phẩm RIÊNG BIỆT, mỗi mã 1 dòng
- Khi tách, TẤT CẢ các trường thông tin chung (ten_san_pham, mo_ta_tinh_nang, kich_thuoc, thuong_hieu, danh_muc...) PHẢI được COPY GIỐNG NHAU cho mọi sản phẩm được tách ra
- Chỉ khác nhau ở ma_san_pham và giá tương ứng của từng mã
- Giá niêm yết và giá bán ra phải thuộc về CÙNG 1 mã sản phẩm, không lấy giá của mã khác

Quy tắc về giá:
- Nếu sản phẩm chỉ có 1 giá duy nhất -> đó là gia_niem_yet, để trống gia_ban_ra
- Nếu sản phẩm có 2 giá CỦA CÙNG 1 MÃ -> giá CAO hơn là gia_niem_yet, giá THẤP hơn là gia_ban_ra

Quy tắc chung:
- CHỈ trích xuất sản phẩm thực sự (có tên + giá). BỎ QUA thông tin tham khảo, bản vẽ kỹ thuật, thông số kỹ thuật, phụ kiện đi kèm miễn phí
- Tất cả giá trị phải là string trong dấu ngoặc kép
- Giá để nguyên số dạng string, không thêm ký tự
- Nếu không có thông tin thì để chuỗi rỗng ""
- CHỈ trả về JSON array, không giải thích gì thêm
- Nếu trang không chứa sản phẩm nào, trả về []

Ví dụ: 1 hình có tên "Lavabo đặt bàn", thương hiệu INAX, danh mục Lavabo, với 2 mã AC-1 (giá 3tr) và AC-2 (giá 3.5tr):
[{"ma_san_pham":"AC-1","ma_phu_kien":"","ten_san_pham":"Lavabo đặt bàn","hinh_anh":"","mo_ta_tinh_nang":"Lavabo đặt bàn cao cấp","kich_thuoc":"500x400mm","thuong_hieu":"INAX","danh_muc":"Lavabo","don_vi":"","thue_vat":"","gia_niem_yet":"3000000","gia_mua_vao":"","gia_ban_ra":""},{"ma_san_pham":"AC-2","ma_phu_kien":"","ten_san_pham":"Lavabo đặt bàn","hinh_anh":"","mo_ta_tinh_nang":"Lavabo đặt bàn cao cấp","kich_thuoc":"500x400mm","thuong_hieu":"INAX","danh_muc":"Lavabo","don_vi":"","thue_vat":"","gia_niem_yet":"3500000","gia_mua_vao":"","gia_ban_ra":""}]"""

    content = [{"type": "text", "text": prompt_text}]
    for b64 in images_b64:
        content.append({"type": "image_url", "image_url": {"url": f"data:image/png;base64,{b64}"}})

    log.info(f"Calling {model} with {len(images_b64)} images...")
    resp = requests.post(
        "https://openrouter.ai/api/v1/chat/completions",
        headers={"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"},
        json={"model": model, "messages": [{"role": "user", "content": content}], "max_tokens": 16000, "temperature": 0.1},
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


def parse_llm_response(raw_text):
    cleaned = raw_text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("\n", 1)[1]
    if cleaned.endswith("```"):
        cleaned = cleaned.rsplit("```", 1)[0]
    cleaned = cleaned.strip()
    try:
        products = json.loads(cleaned)
    except json.JSONDecodeError:
        log.warning("Failed to parse LLM response as JSON")
        return []
    if not isinstance(products, list):
        return []
    return [{h: str(p.get(h, "")) for h in HEADERS} for p in products if isinstance(p, dict)]


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")

@app.post("/upload")
async def upload_pdf(file: UploadFile = File(...)):
    original_name = file.filename or ""
    ext = Path(original_name).suffix.lower()
    if ext not in (".pdf", ".xlsx"):
        return JSONResponse({"error": "Chỉ chấp nhận file .pdf hoặc .xlsx"}, status_code=400)

    file_id = str(uuid.uuid4())[:8]
    filepath = UPLOAD_DIR / f"{file_id}{ext}"
    content = await file.read()
    with open(filepath, "wb") as f:
        f.write(content)

    if ext == ".xlsx":
        try:
            preview = read_excel_preview(str(filepath))
            row_count = preview["row_count"]
        except Exception as e:
            log.error(f"Failed to read Excel file: {e}")
            return JSONResponse({"error": "File Excel không hợp lệ"}, status_code=400)
        return JSONResponse({"file_id": file_id, "filename": original_name, "file_type": "xlsx", "row_count": row_count})
    else:
        doc = fitz.open(str(filepath))
        total_pages = len(doc)
        doc.close()
        return JSONResponse({"file_id": file_id, "filename": original_name, "file_type": "pdf", "total_pages": total_pages, "pdf_url": f"/pdf/{file_id}"})

@app.get("/pdf/{file_id}")
async def serve_pdf(file_id: str):
    filepath = UPLOAD_DIR / f"{file_id}.pdf"
    if not filepath.exists():
        return JSONResponse({"error": "File not found"}, status_code=404)
    return FileResponse(str(filepath), media_type="application/pdf")

@app.get("/xlsx/{file_id}")
async def serve_xlsx(file_id: str):
    filepath = UPLOAD_DIR / f"{file_id}.xlsx"
    if not filepath.exists():
        return JSONResponse({"error": "File not found"}, status_code=404)
    return FileResponse(str(filepath), media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

@app.get("/models")
async def list_models():
    if not API_KEY:
        return JSONResponse({"error": "OPENROUTER_API_KEY not set"}, status_code=500)
    try:
        models = model_cache.get_models(API_KEY)
        return JSONResponse([{"id": m.id, "name": m.name, "prompt_price": m.prompt_price, "completion_price": m.completion_price} for m in models])
    except requests.RequestException as e:
        log.error(f"Failed to fetch models: {e}")
        return JSONResponse({"error": f"OpenRouter API unavailable: {str(e)}"}, status_code=502)

@app.get("/excel-preview/{file_id}")
async def excel_preview(file_id: str):
    filepath = UPLOAD_DIR / f"{file_id}.xlsx"
    if not filepath.exists():
        return JSONResponse({"error": "File not found"}, status_code=404)
    try:
        preview = read_excel_preview(str(filepath))
        return JSONResponse(preview)
    except Exception as e:
        log.error(f"Failed to read Excel preview: {e}")
        return JSONResponse({"error": str(e)}, status_code=500)

EXCEL_LLM_BATCH_SIZE = 100

@app.get("/extract-excel")
async def extract_excel(request: Request):
    file_id = request.query_params.get("file_id")
    model = request.query_params.get("model") or MODEL
    filename = request.query_params.get("filename", "")
    start_row = int(request.query_params.get("start_row", 1))
    end_row = int(request.query_params.get("end_row", 99999))
    batch_size = int(request.query_params.get("batch_size", EXCEL_LLM_BATCH_SIZE))

    filepath = UPLOAD_DIR / f"{file_id}.xlsx"
    if not filepath.exists():
        return JSONResponse({"error": "File not found"}, status_code=404)
    if not API_KEY:
        return JSONResponse({"error": "OPENROUTER_API_KEY not set"}, status_code=500)

    # Ensure models are cached for pricing
    if not model_cache._models and API_KEY:
        try:
            model_cache.get_models(API_KEY)
        except Exception:
            pass
    pricing = model_cache.get_model_pricing(model)
    prompt_price = pricing.prompt_price if pricing else 0.0
    completion_price = pricing.completion_price if pricing else 0.0

    def event_stream():
        session_cost = SessionCost(0, 0, 0.0)
        all_products = []

        try:
            all_classified = classify_rows(str(filepath))
            # Filter by row range (1-based, relative to data rows)
            data_rows_only = [r for r in all_classified if r.row_type not in (RowType.CATEGORY, RowType.SKIP)]
            # Keep categories for danh_muc propagation, filter products by index
            product_index = 0
            classified = []
            for r in all_classified:
                if r.row_type in (RowType.CATEGORY, RowType.SKIP):
                    classified.append(r)
                else:
                    product_index += 1
                    if start_row <= product_index <= end_row:
                        classified.append(r)
            desc_pairs = build_descriptions_batch(classified)
        except Exception as e:
            log.error(f"Excel classification error: {e}")
            yield f"data: {json.dumps({'type': 'error', 'batch': 0, 'message': str(e)}, ensure_ascii=False)}\n\n"
            yield f"data: {json.dumps({'type': 'done', 'record_id': None, 'total_cost': {'prompt_tokens': 0, 'completion_tokens': 0, 'total_cost_usd': 0.0}})}\n\n"
            return

        batches = []
        for i in range(0, len(desc_pairs), batch_size):
            batches.append(desc_pairs[i:i + batch_size])

        total_batches = max(len(batches), 1)
        log.info(f"Starting Excel extraction: {len(classified)} rows, {len(desc_pairs)} descriptions, {total_batches} batches, model={model}")

        llm_results: dict[int, dict] = {}

        if not batches:
            yield f"data: {json.dumps({'type': 'progress', 'batch': 1, 'total_batches': 1, 'descriptions': '0'})}\n\n"
        else:
            for batch_idx, batch_pairs in enumerate(batches):
                desc_start = batch_idx * EXCEL_LLM_BATCH_SIZE + 1
                desc_end = desc_start + len(batch_pairs) - 1
                log.info(f"--- Batch {batch_idx+1}/{total_batches}: descriptions {desc_start}-{desc_end} ---")
                yield f"data: {json.dumps({'type': 'progress', 'batch': batch_idx+1, 'total_batches': total_batches, 'descriptions': f'{desc_start}-{desc_end}'})}\n\n"

                try:
                    descriptions = [desc for _, desc in batch_pairs]
                    row_indices = [idx for idx, _ in batch_pairs]

                    results, usage = call_llm_text_batch(descriptions, model, API_KEY)

                    for row_idx, result in zip(row_indices, results):
                        llm_results[row_idx] = result

                    cost_info = None
                    if usage:
                        batch_cost = calculate_batch_cost(usage, prompt_price, completion_price)
                        if batch_cost:
                            cost_info = {"prompt_tokens": batch_cost.prompt_tokens, "completion_tokens": batch_cost.completion_tokens, "cost_usd": batch_cost.cost_usd}
                            session_cost = accumulate_cost(session_cost, batch_cost)

                    # Send cost update only (no rows yet)
                    yield f"data: {json.dumps({'type': 'batch', 'batch': batch_idx+1, 'headers': OUTPUT_HEADERS, 'rows': [], 'cost': cost_info}, ensure_ascii=False)}\n\n"

                    log.info(f"Batch {batch_idx+1}: processed {len(descriptions)} descriptions")
                except Exception as e:
                    log.error(f"Batch {batch_idx+1} error: {e}")
                    yield f"data: {json.dumps({'type': 'error', 'batch': batch_idx+1, 'message': str(e)}, ensure_ascii=False)}\n\n"

        # Build all output rows AFTER all LLM batches complete
        output_rows = build_output_rows(classified, llm_results)
        all_products = output_rows
        rows = [[p.get('_stt', '')] + [p.get(h, '') for h in OUTPUT_HEADERS] for p in output_rows]
        yield f"data: {json.dumps({'type': 'batch', 'batch': total_batches, 'headers': OUTPUT_HEADERS, 'rows': rows, 'cost': None}, ensure_ascii=False)}\n\n"

        json_data = json.dumps(all_products, ensure_ascii=False)
        record_id = None
        try:
            record = ExtractionRecord(
                id=None, file_id=file_id, filename=filename or f"{file_id}.xlsx",
                model_name=model, start_page=0, end_page=0,
                product_count=len(all_products), json_data=json_data,
                total_cost=session_cost.total_cost_usd if session_cost.total_cost_usd > 0 else None,
                prompt_tokens=session_cost.total_prompt_tokens if session_cost.total_prompt_tokens > 0 else None,
                completion_tokens=session_cost.total_completion_tokens if session_cost.total_completion_tokens > 0 else None,
                created_at=datetime.now().isoformat(sep=" ", timespec="seconds"),
            )
            record_id = save_record(record)
            log.info(f"Saved Excel extraction record #{record_id}")
        except Exception as e:
            log.error(f"Failed to save record: {e}")

        yield f"data: {json.dumps({'type': 'done', 'record_id': record_id, 'total_cost': {'prompt_tokens': session_cost.total_prompt_tokens, 'completion_tokens': session_cost.total_completion_tokens, 'total_cost_usd': session_cost.total_cost_usd}})}\n\n"
        log.info("Excel extraction complete!")

    return StreamingResponse(event_stream(), media_type="text/event-stream", headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache, no-transform", "Connection": "keep-alive"})

@app.get("/extract")
async def extract(request: Request):
    file_id = request.query_params.get("file_id")
    start_page = int(request.query_params.get("start_page", 0))
    end_page = request.query_params.get("end_page")
    model = request.query_params.get("model") or MODEL
    filename = request.query_params.get("filename", "")
    batch_size = int(request.query_params.get("batch_size", BATCH_SIZE))

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

    # Ensure models are cached for pricing
    if not model_cache._models and API_KEY:
        try:
            model_cache.get_models(API_KEY)
        except Exception:
            pass
    pricing = model_cache.get_model_pricing(model)
    prompt_price = pricing.prompt_price if pricing else 0.0
    completion_price = pricing.completion_price if pricing else 0.0

    def event_stream():
        total_batches = (end_page - start_page + batch_size - 1) // batch_size
        log.info(f"Starting extraction: pages {start_page+1}-{end_page}, {total_batches} batches, model={model}")
        session_cost = SessionCost(0, 0, 0.0)
        all_products = []

        for batch_idx, batch_start in enumerate(range(start_page, end_page, batch_size)):
            batch_end = min(batch_start + batch_size, end_page)
            log.info(f"--- Batch {batch_idx+1}/{total_batches}: pages {batch_start+1}-{batch_end} ---")
            yield f"data: {json.dumps({'type': 'progress', 'batch': batch_idx+1, 'total_batches': total_batches, 'pages': f'{batch_start+1}-{batch_end}'})}\n\n"

            try:
                images = pdf_pages_to_base64(str(filepath), batch_start, batch_end)
                raw_response, usage = call_llm(images, model=model)
                products = parse_llm_response(raw_response)
                all_products.extend(products)

                cost_info = None
                if usage:
                    batch_cost = calculate_batch_cost(usage, prompt_price, completion_price)
                    if batch_cost:
                        cost_info = {"prompt_tokens": batch_cost.prompt_tokens, "completion_tokens": batch_cost.completion_tokens, "cost_usd": batch_cost.cost_usd}
                        session_cost = accumulate_cost(session_cost, batch_cost)

                rows = [[p[h] for h in HEADERS] for p in products]
                yield f"data: {json.dumps({'type': 'batch', 'batch': batch_idx+1, 'headers': HEADERS, 'rows': rows, 'pages': f'{batch_start+1}-{batch_end}', 'cost': cost_info}, ensure_ascii=False)}\n\n"
                log.info(f"Batch {batch_idx+1}: extracted {len(products)} products")
            except Exception as e:
                log.error(f"Batch {batch_idx+1} error: {e}")
                yield f"data: {json.dumps({'type': 'error', 'batch': batch_idx+1, 'message': str(e), 'pages': f'{batch_start+1}-{batch_end}'}, ensure_ascii=False)}\n\n"

        json_data = json.dumps(all_products, ensure_ascii=False)
        record_id = None
        try:
            record = ExtractionRecord(
                id=None, file_id=file_id, filename=filename or f"{file_id}.pdf",
                model_name=model, start_page=start_page, end_page=end_page,
                product_count=len(all_products), json_data=json_data,
                total_cost=session_cost.total_cost_usd if session_cost.total_cost_usd > 0 else None,
                prompt_tokens=session_cost.total_prompt_tokens if session_cost.total_prompt_tokens > 0 else None,
                completion_tokens=session_cost.total_completion_tokens if session_cost.total_completion_tokens > 0 else None,
                created_at=datetime.now().isoformat(sep=" ", timespec="seconds"),
            )
            record_id = save_record(record)
            log.info(f"Saved extraction record #{record_id}")
        except Exception as e:
            log.error(f"Failed to save record: {e}")

        yield f"data: {json.dumps({'type': 'done', 'record_id': record_id, 'total_cost': {'prompt_tokens': session_cost.total_prompt_tokens, 'completion_tokens': session_cost.total_completion_tokens, 'total_cost_usd': session_cost.total_cost_usd}})}\n\n"
        log.info("Extraction complete!")

    return StreamingResponse(event_stream(), media_type="text/event-stream", headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache, no-transform", "Connection": "keep-alive"})

@app.get("/history")
async def history(request: Request):
    limit = int(request.query_params.get("limit", 50))
    offset = int(request.query_params.get("offset", 0))
    return JSONResponse(get_history(limit=limit, offset=offset))

@app.get("/history/{record_id}")
async def history_detail(record_id: int):
    record = get_record(record_id)
    if record is None:
        return JSONResponse({"error": "Record not found"}, status_code=404)
    from dataclasses import asdict
    return JSONResponse(asdict(record))

@app.get("/history/{record_id}/excel")
async def history_excel(record_id: int):
    record = get_record(record_id)
    if record is None:
        return JSONResponse({"error": "Record not found"}, status_code=404)
    return _make_excel_response(record.json_data, record.filename)

@app.post("/download-excel")
async def download_excel(request: Request):
    body = await request.json()
    return _make_excel_response(body.get("json_data", "[]"), body.get("filename", "extracted"))

def _make_excel_response(json_data_str: str, filename: str):
    from openpyxl import Workbook
    import io as _io
    wb = Workbook()
    ws = wb.active
    try:
        products = json.loads(json_data_str)
    except json.JSONDecodeError:
        products = []
    if not isinstance(products, list):
        products = []
    ws.append(HEADERS)
    for p in products:
        if isinstance(p, dict):
            ws.append([str(p.get(h, "")) for h in HEADERS])
    output = _io.BytesIO()
    wb.save(output)
    output.seek(0)
    base = filename.rsplit(".", 1)[0] if filename else "extracted"
    return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition": f"attachment; filename*=UTF-8''{urllib.parse.quote(base + '_extracted.xlsx')}"})

@app.put("/history/{record_id}")
async def update_history(record_id: int, request: Request):
    body = await request.json()
    json_data = body.get("json_data", "[]")
    try:
        products = json.loads(json_data)
        product_count = len(products) if isinstance(products, list) else 0
    except json.JSONDecodeError:
        product_count = 0
    updated = update_record_json(record_id, json_data, product_count)
    if not updated:
        return JSONResponse({"error": "Record not found"}, status_code=404)
    return JSONResponse({"ok": True, "product_count": product_count})

@app.delete("/history/{record_id}")
async def delete_history(record_id: int):
    deleted = delete_record(record_id)
    if not deleted:
        return JSONResponse({"error": "Record not found"}, status_code=404)
    return JSONResponse({"ok": True})
