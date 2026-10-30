IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'SchemaMigrationTests')
    BEGIN
        CREATE DATABASE SchemaMigrationTests;
        ALTER DATABASE SchemaMigrationTests set CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);
    END;
GO

IF NOT EXISTS (SELECT 1 FROM [SchemaMigrationTests].sys.schemas WHERE name = 'shards')
BEGIN
    EXEC [SchemaMigrationTests].sys.sp_executesql N'CREATE SCHEMA [shards];'
END;
GO

IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'StreamRunnerTests')
    BEGIN
            CREATE DATABASE StreamRunnerTests;
            ALTER DATABASE StreamRunnerTests set CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);
    END;
GO

IF NOT EXISTS (SELECT 1 FROM [StreamRunnerTests].sys.schemas WHERE name = 'shards')
BEGIN
    EXEC [StreamRunnerTests].sys.sp_executesql N'CREATE SCHEMA [shards];'
END;
GO