FROM python:3.11-slim

# 必須コマンド + 日本語フォント
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    libreoffice \
    poppler-utils \
    fonts-noto-cjk \
    fonts-noto-cjk-extra \
    locales \
    && rm -rf /var/lib/apt/lists/*

# フォントキャッシュ（起動遅延回避のためビルド時に実施）
RUN fc-cache -f -v || true

# 日本語ロケール
RUN sed -i 's/# *ja_JP.UTF-8/ja_JP.UTF-8/' /etc/locale.gen && locale-gen
ENV LANG=ja_JP.UTF-8 LANGUAGE=ja_JP:ja LC_ALL=ja_JP.UTF-8

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY main.py .

# ★ Cloud Run の PORT をそのまま使用し、lifespan を無効化（起動即 listen）
ENV PYTHONUNBUFFERED=1
CMD exec uvicorn main:app --host 0.0.0.0 --port ${PORT} --workers 1 --lifespan off --log-level info
