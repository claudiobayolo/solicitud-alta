FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y \
    curl gnupg unixodbc-dev locales build-essential gcc g++ apt-transport-https wget \
    && curl -sSL https://packages.microsoft.com/config/debian/12/packages-microsoft-prod.deb -o packages-microsoft-prod.deb \
    && dpkg -i packages-microsoft-prod.deb \
    && rm packages-microsoft-prod.deb \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .
RUN pip install --no-cache-dir --upgrade pip && pip install -r requirements.txt

EXPOSE $PORT
CMD ["gunicorn", "--bind", "0.0.0.0:$PORT", "flask_app:app"]
