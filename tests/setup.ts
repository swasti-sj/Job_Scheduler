// Point every integration test at the docker-compose.test.yml services before
// src/config.ts is imported and frozen.
process.env['DATABASE_URL'] ??= 'postgres://scheduler:scheduler@localhost:5433/scheduler_test';
process.env['REDIS_URL'] ??= 'redis://localhost:6380';
process.env['LOG_LEVEL'] ??= 'error';
