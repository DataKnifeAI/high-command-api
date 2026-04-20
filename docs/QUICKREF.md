# Quick Reference

## Getting Started

### Installation
```bash
# Clone repository
git clone https://github.com/DataKnifeAI/high-command-api.git
cd high-command-api

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Running the API
```bash
# Using main.py
python main.py

# Using uvicorn directly
uvicorn src.app:app --reload

# Using Makefile
make dev
```

### Access Points
- **API**: http://localhost:5000
- **Swagger UI**: http://localhost:5000/docs
- **ReDoc**: http://localhost:5000/redoc
- **Health Check**: http://localhost:5000/api/health

## Common Endpoints

### Get Current Status
```bash
# War status
curl http://localhost:5000/api/war/status

# All planets
curl http://localhost:5000/api/planets

# Statistics
curl http://localhost:5000/api/statistics

# All factions
curl http://localhost:5000/api/factions

# All biomes
curl http://localhost:5000/api/biomes
```

### Specific Planet
```bash
# Planet status
curl http://localhost:5000/api/planets/0

# Planet history (last 20 records)
curl "http://localhost:5000/api/planets/0/history?limit=20"
```

### Refresh Data
```bash
# Refresh war status
curl -X POST http://localhost:5000/api/war/status/refresh

# Refresh statistics
curl -X POST http://localhost:5000/api/statistics/refresh
```

## Database

### Reset Database
```bash
make db-reset
```

### Check Database
```bash
# View database file
ls -lh helldivers2.db

# Query with sqlite3
sqlite3 helldivers2.db "SELECT COUNT(*) FROM war_status;"
```

## Docker

### Build Image
```bash
docker build -t high-command-api .
```

### Run Container
```bash
# Standalone
docker run -d -p 5000:5000 \
  -v $(pwd)/helldivers2.db:/app/helldivers2.db \
  --name high-command-api \
  high-command-api

# Using docker-compose
docker-compose up -d
```

### Container Operations
```bash
# View logs
docker-compose logs -f

# Stop services
docker-compose down

# Remove volumes
docker-compose down -v
```

## Testing

### Run Tests
```bash
# Using Python directly
python -m tests.demo

# Using Makefile
make test

# With coverage
pytest tests/ --cov=src
```

### Manual Testing
```bash
# Health check
curl http://localhost:5000/api/health

# Get war status
curl -s http://localhost:5000/api/war/status | jq

# Test docs
curl http://localhost:5000/openapi.json | jq
```

## Development

### Code Quality
```bash
# Format code
make format

# Lint code
make lint

# Type checking
make check
```

### Environment Configuration
```bash
# Copy example
cp .env.example .env

# Edit as needed
nano .env
```

### Key Settings
- `FLASK_ENV`: Set to `development`, `production`, or `testing`
- `LOG_LEVEL`: Set logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`)
- `DATABASE_URL`: SQLite database path
- `SCRAPE_INTERVAL`: Data collection interval in seconds (default: 300)

## Makefile Targets

### Setup
```bash
make venv      # Create virtual environment
make install   # Install dependencies
make clean     # Clean build artifacts
```

### Development
```bash
make dev       # Run in development mode
make prod      # Run in production mode
make test      # Run tests
```

### Docker
```bash
make docker-build   # Build Docker image
make docker-up      # Start containers
make docker-down    # Stop containers
make docker-logs    # View logs
```

### Code Quality
```bash
make lint      # Run linters
make format    # Format code
make check     # Type checking
```

### Utilities
```bash
make health    # Check API health
make info      # Display project info
make help      # Show all targets
```

## Troubleshooting

### API Not Starting
```bash
# Check if port 5000 is in use
lsof -i :5000

# Kill process on port 5000
kill -9 $(lsof -t -i:5000)

# Try different port
python main.py --port 8000
```

### Database Locked
```bash
# Reset database
make db-reset

# Or remove and recreate
rm helldivers2.db
python -c "from src.database import Database; Database()"
```

### Import Errors
```bash
# Reinstall dependencies
pip install -r requirements.txt --force-reinstall

# Verify package installation
pip list | grep fastapi
```

### Collector Not Running
```bash
# Check API health
curl http://localhost:5000/api/health

# Check logs
docker-compose logs api

# Restart service
docker-compose restart api
```

## Directory Structure

```
high-command-api/
├── src/                    # Application code
├── tests/                  # Test suite
├── docs/                   # Documentation
├── main.py                # Entry point
├── Makefile               # Automation
├── Dockerfile             # Container
├── requirements.txt       # Dependencies
├── .env.example           # Config template
└── README.md              # Overview
```

## Configuration Files

- `.env` - Environment variables (create from .env.example)
- `pyproject.toml` - Project metadata
- `docker-compose.yml` - Docker services
- `Dockerfile` - Container image
- `Makefile` - Build targets

## Useful Commands

### View All Make Targets
```bash
make help
```

### Get Project Information
```bash
make info
```

### Run All Linters
```bash
make lint
```

### Format All Python Files
```bash
make format
```

### Check Types
```bash
make check
```

### Push to GitHub
```bash
make push
```

### View Git Status
```bash
make status
```

## Performance Tips

1. **Increase Collection Interval**: Reduce frequency of background tasks
2. **Database Indexing**: Already optimized for common queries
3. **Connection Pooling**: HTTP sessions reuse connections
4. **Async Operations**: All endpoints are non-blocking
5. **Caching**: Consider implementing for frequently accessed data

## Security Reminders

- Never commit `.env` file with real credentials
- Use strong database passwords in production
- Enable HTTPS in production environments
- Implement rate limiting for public APIs
- Keep dependencies updated regularly

## Useful Resources

- **FastAPI**: https://fastapi.tiangolo.com/
- **Uvicorn**: https://www.uvicorn.org/
- **APScheduler**: https://apscheduler.readthedocs.io/
- **SQLite**: https://www.sqlite.org/docs.html
- **Hell Divers 2 API**: https://api.live.prod.theadultswim.com/helldivers2

## Getting Help

- Check the documentation in `/docs` folder
- Review API reference at `/docs/API.md`
- View architecture at `/docs/ARCHITECTURE.md`
- Run tests to verify setup: `make test`
- Check GitHub issues: https://github.com/DataKnifeAI/high-command-api/issues
