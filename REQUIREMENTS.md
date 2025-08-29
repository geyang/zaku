# Requirements and Dependencies

This document describes the different requirements files available for the Zaku project.

## Requirements Files

### `requirements.txt` - Standard Installation
Contains all core dependencies needed to run Zaku with full functionality:
```bash
pip install -r requirements.txt
```

**Includes:**
- Core functionality (params-proto, msgpack, numpy, websockets)
- Database drivers (MongoDB async and sync)
- HTTP server capabilities (aiohttp)
- Redis for task queue
- Image processing (Pillow)
- Utility tools (killport)

### `requirements-minimal.txt` - Production Minimal
Contains only the essential dependencies for production deployment:
```bash
pip install -r requirements-minimal.txt
```

**Includes:**
- Core functionality (params-proto, msgpack, numpy, websockets)
- Database drivers (MongoDB)
- Redis for task queue
- Utility tools (killport)

**Excludes:**
- Image processing (Pillow)
- HTTP server (aiohttp)
- CORS support

### `requirements-dev.txt` - Development Environment
Contains all dependencies needed for development, testing, and documentation:
```bash
pip install -r requirements-dev.txt
```

**Includes:**
- All core dependencies
- Testing frameworks (pytest, pytest-asyncio)
- Code quality tools (black, pylint, ruff)
- Documentation tools (sphinx, furo)
- Advanced features (opencv, open3d, trimesh)

## Installation Options

### For End Users
```bash
# Full installation with all features
pip install -r requirements.txt

# Minimal installation for production
pip install -r requirements-minimal.txt
```

### For Developers
```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Or install core + dev separately
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### For Contributors
```bash
# Install in editable mode with dev dependencies
pip install -e .
pip install -r requirements-dev.txt
```

## Version Compatibility

- **Python**: >= 3.7
- **MongoDB**: >= 4.0.0
- **Redis**: Latest stable version
- **NumPy**: >= 1.21 (for numpy.typing.NDArray support)

## Notes

- `asyncio` is included in Python 3.7+ standard library
- `pathlib` is included in Python 3.4+ standard library
- Some dependencies like `opencv-python` and `open3d` are optional and only needed for specific examples
- The project uses `pyproject.toml` for build configuration, but these requirements files provide easy pip installation 