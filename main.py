import fitz  # PyMuPDF
import base64
import requests
import json
import sys
import os

API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
MODEL = "anthropic/claude-sonnet-4"
import glob
_parent = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
_matches = glob.glob(os.path.join(_parent, "*.pdf"))
PDF_PATH = _matches[0] if _matches else ""


def pdf_pages_to_base64(pdf_path, start=0, end=10):
    doc = fitz.open(pdf_path)
    images = []
    for i in range(start, min(end, len(doc))):
        page = doc[i]
        pix = page.get_pixmap(dpi=200)
        img_bytes = pix.tobytes("png")
        b64 = base64.b64encode(img_bytes).decode("utf-8")
        images.append(b64)
        print(f"  Page {i+1} converted ({len(img_bytes)//1024} KB)")
    doc.close()
    return images


def extract_products(images_b64):
    content = [
        {
            "type": "text",
            "text": """Bạn là chuyên gia trích xuất dữ liệu từ catalogue sản phẩm.
Hãy xem tất cả các trang catalogue dưới đây và trích xuất TOÀN BỘ sản phẩm thành bảng CSV với các cột:
- ma_san_pham: Mã sản phẩm (model number/code)
- ten_san_pham: Tên sản phẩm
- gia_niem_yet: Giá niêm yết (nếu có)
- gia_ban: Giá bán (nếu có)
- brand: Thương hiệu (mặc định INAX nếu không ghi rõ)
- category: Danh mục sản phẩm (ví dụ: bồn cầu, lavabo, vòi sen, phụ kiện...)

Quy tắc:
- Trả về ĐÚNG format CSV, dùng dấu phẩy ngăn cách
- Dòng đầu tiên là header
- Nếu không có thông tin thì để trống
- Giá để nguyên số, không thêm ký tự
- Mỗi sản phẩm một dòng
- CHỈ trả về CSV, không giải thích gì thêm"""
        }
    ]

    for i, b64 in enumerate(images_b64):
        content.append({
            "type": "image_url",
            "image_url": {
                "url": f"data:image/png;base64,{b64}"
            }
        })

    print(f"\nSending {len(images_b64)} pages to {MODEL}...")

    response = requests.post(
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
        timeout=180,
    )

    if response.status_code != 200:
        print(f"API Error {response.status_code}: {response.text}")
        sys.exit(1)

    result = response.json()
    return result["choices"][0]["message"]["content"]


def main():
    if not API_KEY:
        print("Error: Set OPENROUTER_API_KEY environment variable")
        sys.exit(1)

    print(f"Processing: {PDF_PATH}")
    print(f"Converting first 10 pages to images...")

    images = pdf_pages_to_base64(PDF_PATH, 0, 10)

    csv_text = extract_products(images)

    # Clean up markdown code fences if present
    csv_text = csv_text.strip()
    if csv_text.startswith("```"):
        csv_text = csv_text.split("\n", 1)[1]
    if csv_text.endswith("```"):
        csv_text = csv_text.rsplit("```", 1)[0]
    csv_text = csv_text.strip()

    output_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "products_10pages.csv")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(csv_text)

    print(f"\nOutput saved to: {output_path}")
    print(f"\nPreview:")
    print("-" * 80)
    for line in csv_text.split("\n")[:20]:
        print(line)
    total_lines = csv_text.count("\n") + 1
    if total_lines > 20:
        print(f"... ({total_lines} total lines)")


if __name__ == "__main__":
    main()
