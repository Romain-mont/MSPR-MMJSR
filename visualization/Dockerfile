FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    streamlit>=1.40.0 \
    pandas>=2.0.0 \
    matplotlib>=3.7.0 \
    seaborn>=0.13.0 \
    requests>=2.32.0 \
    python-dotenv>=1.0.0

COPY visualization/ ./visualization/

EXPOSE 8501

HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health')"

CMD ["streamlit", "run", "visualization/dashboard_web.py", \
     "--server.port=8501", \
     "--server.address=0.0.0.0", \
     "--server.headless=true"]
