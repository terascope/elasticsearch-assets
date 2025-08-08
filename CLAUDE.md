# Elasticsearch Assets - Claude Knowledge

## Quick Reference

### Development Commands

```bash
yarn install          # Install dependencies
yarn build            # Build TypeScript
yarn build:watch      # Build with file watching
```

### Testing

```bash
yarn test             # Run tests (requires Docker for Elasticsearch)
yarn test:watch       # Run tests in watch mode
yarn test:debug       # Run tests with debugging
```

**Note**: Tests require Docker to spin up Elasticsearch/OpenSearch instances.

### Linting

```bash
yarn lint             # Check code style with ESLint
yarn lint:fix         # Auto-fix linting issues
```

Uses `@terascope/eslint-config` with stylistic rules for spacing and line breaks.

## Project Structure

### Key Directories

- `asset/src/` - Main asset operations (spaces_reader, elasticsearch_reader, etc.)
- `packages/elasticsearch-asset-apis/` - Core API implementations
- `test/` - Test files mirroring src structure

### Main Components

- **Readers**: `spaces_reader`, `elasticsearch_reader`, `id_reader`
- **Base Classes**: `ReaderAPIFetcher`, `DateReaderAPISlicer`, `ElasticsearchReaderAPI`
- **Clients**: `SpacesReaderClient`, `ElasticsearchReaderClient`

## Error Handling Architecture

Reader operations flow: `Slicer.initialize()` → `ElasticsearchReaderAPI.makeDateSlicerRanges()` → `client.count()` → potential errors

Errors during slicer initialization can cause jobs to silently "complete" instead of failing properly.