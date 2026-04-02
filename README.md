# PDF Product Extractor

Trích xuất thông tin sản phẩm từ catalogue PDF sử dụng AI (OpenRouter API).

## Yêu cầu

- Docker & Docker Compose
- OpenRouter API Key ([lấy tại đây](https://openrouter.ai/keys))

## Cài đặt & Chạy

1. Tạo file `.env`:

```bash
cp .env.example .env
```

2. Điền API key vào `.env`:

```
OPENROUTER_API_KEY=sk-or-v1-your-key-here
```

3. Chạy:

```bash
docker compose up -d
```

4. Mở trình duyệt: http://localhost:8000

## Chạy không dùng Docker (uv)

```bash
uv run python -m uvicorn app:app --host 0.0.0.0 --port 8000
```

## Dừng

```bash
docker compose down
```

## Cách dùng

1. Upload file PDF catalogue
2. Chọn phạm vi trang cần trích xuất
3. Bấm "Trích xuất"
4. Tải kết quả CSV
