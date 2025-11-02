/*
====================================================
Data Warehouse Initialization Script
====================================================
Description:
- Checks if the 'DataWarehouse' database exists
- Drops and recreates it (⚠️ all data will be lost)
- Creates schemas: bronze, silver, gold
====================================================
*/

USE master;

-- Drop existing DataWarehouse safely
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    PRINT '⚠️ Database [DataWarehouse] exists. Dropping it now...';
    ALTER DATABASE [DataWarehouse] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [DataWarehouse];
    PRINT '✅ Database [DataWarehouse] dropped successfully.';
END
GO

-- Create new DataWarehouse
CREATE DATABASE [DataWarehouse];
GO

USE [DataWarehouse];
GO

-- Create Schemas if not exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
    EXEC('CREATE SCHEMA bronze AUTHORIZATION dbo;');
GO
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
    EXEC('CREATE SCHEMA silver AUTHORIZATION dbo;');
GO
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold AUTHORIZATION dbo;');
GO

PRINT '🏗️  Database [DataWarehouse] and schemas [bronze], [silver], [gold] created successfully!';




