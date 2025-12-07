## Details
1. Performed a task.
2. Implemented something else.
3. Added another thing.

## Quality Checklist
- [ ] Known issues created on GitHub
- [ ] Sensitive credentials not included (.env, appsettings.json)
- [ ] README & usage notes updated
- [ ] Comments (docstrings)

## Manual Checks
- [ ] Python pipeline runs with simplified console output (no debug code).
- [ ] Save console to output.txt
- [ ] Web app loads without errors or severe visual bugs (POST Metrics + Neo4j Graph).
- [ ] No warnings from Doxygen
- [ ] Fixed type hints with mypy
- [ ] Code formatted with black, but whitespace preserved for readability
- [ ] Imports formatted with isort

## Automatic via GitHub Actions
- Pass all PyTests
- Generate code documentation with Doxygen
