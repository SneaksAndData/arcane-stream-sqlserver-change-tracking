IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'IntegrationTests')
    BEGIN
        CREATE DATABASE IntegrationTests;
        ALTER DATABASE IntegrationTests set CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON);
    END;
GO

use IntegrationTests;
DROP TABLE IF EXISTS IntegrationTests.dbo.TestTable

CREATE TABLE IntegrationTests.dbo.TestTable
(
    Id int not null primary key,
    Name nvarchar(50) not null,
    d datetime null
);
GO


use IntegrationTests;
ALTER TABLE IntegrationTests.dbo.TestTable ENABLE CHANGE_TRACKING;
GO

DECLARE @i int = 0
WHILE @i < 300
    BEGIN
        SET @i = @i + 1
        INSERT INTO IntegrationTests.dbo.TestTable (Id, Name, d) VALUES (@i, 'Name' + CAST(@i as nvarchar(50)), null)
    END
GO

