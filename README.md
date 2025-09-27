# Cloud Run Document Merge Service

This project exposes a FastAPI application that patches Word (`.docx`) or Excel (`.xlsx`) templates using simple placeholder syntax and renders the result to PDF/JPEGs. It can also return the modified Office document alongside the generated outputs.

## Prerequisites

* Python 3.11
* System tools used by the conversion pipeline:
  * `soffice` (LibreOffice) for Office → PDF conversion
  * `pdftoppm` for PDF → JPEG conversion

Install Python dependencies:

```bash
pip install -r requirements.txt
```

## Running the API locally

```bash
uvicorn main:app --reload --port 8080
```

The server exposes a health check at `GET /healthz` and the main processing endpoint at `POST /merge`.

## `/merge` endpoint

| Field | Location | Type | Description |
|-------|----------|------|-------------|
| `file` | form-data | file | `.docx` or `.xlsx` template upload |
| `file_data_uri` | form-data | string | Alternative to `file`: template as data URI or Base64 |
| `file_url` | form-data | string | Alternative to `file`: URL pointing to the template |
| `mapping_text` | form-data | string | Placeholder/value mapping (see below) |
| `filename` | form-data | string | Base name for generated files (defaults to `document`) |
| `jpeg_dpi` | form-data | int | JPEG output DPI (default `150`) |
| `jpeg_pages` | form-data | string | Comma-separated page numbers/ranges (default `1`) |
| `return_pdf` | form-data | bool | When `true`, include the merged PDF data URI |
| `return_jpegs` | form-data | bool | When `true`, include JPEG previews |
| `return_document` | form-data | bool | When `true`, include the patched `.docx`/`.xlsx` data URI |
| `X-Auth-Id` | header | string | Optional auth token (see below) |
| `Authorization` | header | string | Alternative header for the auth token (`Bearer <token>`) |

Provide the Office template either as a multipart file upload (`file`), a data URI/Base64 string (`file_data_uri`), or a downloadable URL (`file_url`). Only one source is required. Responses are JSON. Depending on the selected flags the payload can contain `pdf_data_uri`, `jpeg_data_uris`, and/or `document_data_uri` entries. All binary payloads are returned as data URIs with appropriate MIME types.

### Authentication

Authentication is configurable via the `AUTH_MODE` environment variable:

* `disabled` (default) — no authentication is enforced.
* `optional` — the service attempts to validate the supplied token but continues when it is missing or invalid; warnings are surfaced in the `diagnostics.auth_warning` field.
* `required` — requests without a valid token receive `401` responses.

Tokens can be supplied via the `X-Auth-Id` header or `Authorization: Bearer <token>`. When validation is enabled the service contacts the external verifier configured by `AUTH_API_BASE_URL` (defaulting to the production endpoint). If the verifier is unavailable and `AUTH_ALLOW_ON_UNAVAILABLE` is `true` (the default) the request proceeds with a warning instead of failing with `503`.

### Placeholder syntax

* Text placeholders: `{customer_name}`
* Numeric-friendly placeholders: when the replacement text is a pure number or percentage the value is written as a number in Excel cells.
* Image placeholders: `{[logo]}` or `{[logo:40mm]}`. The optional size is applied to the image width.
* Loop placeholders (Word and Excel): wrap the repeated block with `{group:loop}` … `#end` and reference loop fields inside the block as `{group:loop:field}`.

`mapping_text` accepts comma- or newline-separated pairs:

```
{customer_name}:Alice Example
{order_total}:12,345
{[logo]}:https://example.com/logo.png
{items:loop:item}:Widget A
{items:loop:price}:1000
{items:loop:item}:Widget B
{items:loop:price}:2500
```

Loop entries can be listed multiple times per field. The service groups entries that share the same loop name (`items` in the example above) and feeds them to templates in row order. In Word templates the section between `{items:loop}` and `#end` repeats for every row, while Excel templates duplicate the rows enclosed by the same markers. Fields missing values in a particular row default to empty strings.

You can also inline newline substitutions using `<br>` inside the replacement text.

### Supplying image data

Images referenced from `{[tag]}` can be provided in several formats:

* HTTP/HTTPS URL (the server downloads the binary data)
* Data URI (`data:image/png;base64,...`)
* Raw Base64 string prefixed with `base64:` or `base64,`

If the download fails or the data cannot be decoded the placeholder is left untouched.

### Example `curl` request

```bash
curl -X POST http://localhost:8080/merge \
  -F 'file=@template.xlsx' \
  -F 'mapping_text={name}:Alice,{[logo]}:data:image/png;base64,iVBORw0...' \
  -F 'filename=report' \
  -F 'return_pdf=true' \
  -F 'return_jpegs=false' \
  -F 'return_document=true'
```

## Testing

The project currently relies on Python's bytecode compilation as a smoke test:

```bash
python -m compileall main.py
```

## Deployment

The application is packaged for Google Cloud Run via the provided `Dockerfile`. Ensure the runtime image has LibreOffice and Poppler (`pdftoppm`) installed.
