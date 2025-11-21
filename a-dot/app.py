from flask import Flask
from flask_openapi3 import OpenAPI, Info

info = Info(title="A-Dot API", version="1.0.0")
app = OpenAPI(__name__, info=info)


@app.get("/")
def index():
    """Root endpoint"""
    return {"message": "Welcome to A-Dot API"}


@app.get("/health")
def health():
    """Health check endpoint"""
    return {"status": "healthy"}


if __name__ == "__main__":
    app.run(debug=True)
