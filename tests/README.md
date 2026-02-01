# Tests Module

## Responsibility
Unit and integration tests for all project modules.

## Why This Exists
Tests ensure:
- Code works as expected
- Changes don't break existing functionality
- Edge cases are handled
- Refactoring is safe

## Test Structure
```
tests/
├── unit/                  # Unit tests (test individual functions)
│   ├── test_extractors.py
│   ├── test_standardisation.py
│   ├── test_validation.py
│   └── ...
├── integration/           # Integration tests (test module interactions)
│   ├── test_pipeline.py
│   └── ...
└── fixtures/              # Test data and fixtures
    └── sample_excel_files/
```

## Running Tests
```bash
# Run all tests
pytest

# Run specific test file
pytest tests/unit/test_extractors.py

# Run with coverage
pytest --cov=.
```

## Future Components
- Unit tests for each module
- Integration tests for end-to-end pipeline
- Test fixtures (sample Excel files, mock data)
