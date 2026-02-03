FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    curl gnupg2 unixodbc-dev locales \
    build-essential gcc g++ \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && chmod +x /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 mssql-tools18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .
RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 10000
CMD ["gunicorn", "--bind", "0.0.0.0:$PORT", "flask_app:app"]


