# A-Dot API

A simple Flask API with OpenAPI support.

## Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Root endpoint |
| `/health` | GET | Health check |

## OpenAPI Documentation

When the application is running, OpenAPI documentation is available at:

| URL | Description |
|-----|-------------|
| `/openapi/openapi.json` | OpenAPI JSON schema |
| `/openapi/swagger` | Swagger UI (interactive, test APIs directly) |
| `/openapi/redoc` | ReDoc UI (clean three-panel documentation) |

## Running Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Run the application
python app.py
```

The API will be available at `http://localhost:5000`.

## Running with Docker

```bash
# Build the image
docker build -t a-dot .

# Run the container
docker run -p 5000:5000 a-dot
```

The API will be available at `http://localhost:5000`.
