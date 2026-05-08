/*
    Churn Prediction Data Warehouse
    Target DBMS: Microsoft SQL Server

    This script is idempotent:
    - creates schemas and tables if missing
    - creates or alters stored procedures
    - seeds current model/threshold config

    Source workbook: data/ver1.xlsx, sheet Data Model
    Current production model: random_forest_calibrated
*/

USE master;
GO

IF DB_ID(N'ChurnDW') IS NULL
BEGIN
    EXEC('CREATE DATABASE ChurnDW');
    PRINT 'Database ChurnDW created successfully.';
END;
GO

USE ChurnDW;
GO

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'stg') EXEC(N'CREATE SCHEMA stg');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'dw') EXEC(N'CREATE SCHEMA dw');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'cfg') EXEC(N'CREATE SCHEMA cfg');
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'audit') EXEC(N'CREATE SCHEMA audit');
GO

/* ============================================================
   1. CONFIG AND AUDIT
   ============================================================ */

IF OBJECT_ID(N'cfg.DwParameter', N'U') IS NULL
BEGIN
    CREATE TABLE cfg.DwParameter
    (
        ParameterName  SYSNAME NOT NULL CONSTRAINT PK_DwParameter PRIMARY KEY,
        ParameterValue NVARCHAR(4000) NOT NULL,
        Description    NVARCHAR(1000) NULL,
        UpdatedAt      DATETIME2(0) NOT NULL CONSTRAINT DF_DwParameter_UpdatedAt DEFAULT SYSUTCDATETIME()
    );
END;
GO

IF OBJECT_ID(N'cfg.ModelVersion', N'U') IS NULL
BEGIN
    CREATE TABLE cfg.ModelVersion
    (
        ModelVersionId INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_ModelVersion PRIMARY KEY,
        ModelName      NVARCHAR(100) NOT NULL,
        ModelVersion   NVARCHAR(100) NOT NULL,
        FeatureSet     NVARCHAR(100) NOT NULL,
        LookbackDays   INT NOT NULL,
        RocAuc         DECIMAL(12,10) NULL,
        AveragePrecision DECIMAL(12,10) NULL,
        Threshold      DECIMAL(18,15) NULL,
        IsActive       BIT NOT NULL CONSTRAINT DF_ModelVersion_IsActive DEFAULT 0,
        Notes          NVARCHAR(2000) NULL,
        CreatedAt      DATETIME2(0) NOT NULL CONSTRAINT DF_ModelVersion_CreatedAt DEFAULT SYSUTCDATETIME(),
        CONSTRAINT UQ_ModelVersion UNIQUE (ModelName, ModelVersion)
    );
END;
GO

IF OBJECT_ID(N'audit.EtlBatch', N'U') IS NULL
BEGIN
    CREATE TABLE audit.EtlBatch
    (
        LoadBatchId    UNIQUEIDENTIFIER NOT NULL CONSTRAINT PK_EtlBatch PRIMARY KEY,
        SourceFileName NVARCHAR(500) NULL,
        PipelineName   NVARCHAR(200) NOT NULL,
        StartedAt      DATETIME2(0) NOT NULL CONSTRAINT DF_EtlBatch_StartedAt DEFAULT SYSUTCDATETIME(),
        FinishedAt     DATETIME2(0) NULL,
        Status         NVARCHAR(30) NOT NULL CONSTRAINT DF_EtlBatch_Status DEFAULT N'RUNNING',
        RowsLoaded     INT NULL,
        ErrorMessage   NVARCHAR(4000) NULL
    );
END;
GO

CREATE OR ALTER PROCEDURE cfg.sp_SetDefaultConfig
AS
BEGIN
    SET NOCOUNT ON;

    MERGE cfg.DwParameter AS tgt
    USING (VALUES
        (N'LookbackDays', N'90', N'Model feature lookback window in days.'),
        (N'UrgentThreshold', N'0.538023093024565', N'Random Forest calibrated threshold selected from November backtest.'),
        (N'HighThreshold', N'0.35', N'Risk level high threshold.'),
        (N'MediumThreshold', N'0.20', N'Risk level medium threshold.'),
        (N'ActiveModelName', N'random_forest_calibrated', N'Current production model.'),
        (N'ActiveModelVersion', N'rf_calibrated_2026_05_08', N'Current production model version.')
    ) AS src(ParameterName, ParameterValue, Description)
    ON tgt.ParameterName = src.ParameterName
    WHEN MATCHED THEN
        UPDATE SET ParameterValue = src.ParameterValue, Description = src.Description, UpdatedAt = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
        INSERT (ParameterName, ParameterValue, Description)
        VALUES (src.ParameterName, src.ParameterValue, src.Description);

    UPDATE cfg.ModelVersion SET IsActive = 0;

    MERGE cfg.ModelVersion AS tgt
    USING (VALUES
        (N'random_forest_calibrated', N'rf_calibrated_2026_05_08', N'customer_snapshot_v1', 90,
         CAST(0.9303990253 AS DECIMAL(12,10)), CAST(0.9595044610 AS DECIMAL(12,10)),
         CAST(0.538023093024565 AS DECIMAL(18,15)), CAST(1 AS BIT),
         N'Selected over XGBoost and Logistic Regression on 2023-10-31 snapshot / November holdout.')
    ) AS src(ModelName, ModelVersion, FeatureSet, LookbackDays, RocAuc, AveragePrecision, Threshold, IsActive, Notes)
    ON tgt.ModelName = src.ModelName AND tgt.ModelVersion = src.ModelVersion
    WHEN MATCHED THEN
        UPDATE SET FeatureSet = src.FeatureSet, LookbackDays = src.LookbackDays, RocAuc = src.RocAuc,
                   AveragePrecision = src.AveragePrecision, Threshold = src.Threshold, IsActive = src.IsActive,
                   Notes = src.Notes
    WHEN NOT MATCHED THEN
        INSERT (ModelName, ModelVersion, FeatureSet, LookbackDays, RocAuc, AveragePrecision, Threshold, IsActive, Notes)
        VALUES (src.ModelName, src.ModelVersion, src.FeatureSet, src.LookbackDays, src.RocAuc,
                src.AveragePrecision, src.Threshold, src.IsActive, src.Notes);
END;
GO

CREATE OR ALTER PROCEDURE audit.sp_StartEtlBatch
    @PipelineName NVARCHAR(200),
    @SourceFileName NVARCHAR(500) = NULL,
    @LoadBatchId UNIQUEIDENTIFIER OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @LoadBatchId = NEWID();

    INSERT INTO audit.EtlBatch (LoadBatchId, SourceFileName, PipelineName)
    VALUES (@LoadBatchId, @SourceFileName, @PipelineName);
END;
GO

CREATE OR ALTER PROCEDURE audit.sp_FinishEtlBatch
    @LoadBatchId UNIQUEIDENTIFIER,
    @Status NVARCHAR(30),
    @RowsLoaded INT = NULL,
    @ErrorMessage NVARCHAR(4000) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE audit.EtlBatch
    SET FinishedAt = SYSUTCDATETIME(),
        Status = @Status,
        RowsLoaded = @RowsLoaded,
        ErrorMessage = @ErrorMessage
    WHERE LoadBatchId = @LoadBatchId;
END;
GO

EXEC cfg.sp_SetDefaultConfig;
GO

/* ============================================================
   2. STAGING
   ============================================================ */

IF OBJECT_ID(N'stg.SalesRaw', N'U') IS NULL
BEGIN
    CREATE TABLE stg.SalesRaw
    (
        SalesRawId        BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_SalesRaw PRIMARY KEY,
        LoadBatchId       UNIQUEIDENTIFIER NULL,
        SourceFileName    NVARCHAR(500) NULL,
        SourceRowNumber   INT NOT NULL,
        ProductCode       NVARCHAR(100) NOT NULL,
        ProductName       NVARCHAR(500) NULL,
        UnitName          NVARCHAR(50) NULL,
        ImportAmount      DECIMAL(18,2) NULL,
        PromoQuantity     DECIMAL(18,4) NULL,
        SalesQuantity     DECIMAL(18,4) NULL,
        ExportAmount      DECIMAL(18,2) NULL,
        Revenue           DECIMAL(18,2) NOT NULL,
        SaleDate          DATE NOT NULL,
        CustomerName      NVARCHAR(255) NOT NULL,
        SegmentName       NVARCHAR(100) NULL,
        Profit            DECIMAL(18,2) NOT NULL,
        MarginPct         DECIMAL(18,8) NULL,
        TotalQuantitySold DECIMAL(18,4) NULL,
        PromoPct          DECIMAL(18,8) NULL,
        YearNumber        INT NULL,
        MonthNumber       INT NULL,
        CategoryName      NVARCHAR(100) NULL,
        RowHash           VARBINARY(32) NULL,
        LoadedAt          DATETIME2(0) NOT NULL CONSTRAINT DF_SalesRaw_LoadedAt DEFAULT SYSUTCDATETIME()
    );

    CREATE INDEX IX_SalesRaw_Batch ON stg.SalesRaw (LoadBatchId);
    CREATE INDEX IX_SalesRaw_Row ON stg.SalesRaw (SourceRowNumber);
END;
GO

IF OBJECT_ID(N'stg.ChurnScoreRaw', N'U') IS NULL
BEGIN
    CREATE TABLE stg.ChurnScoreRaw
    (
        ChurnScoreRawId BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_ChurnScoreRaw PRIMARY KEY,
        LoadBatchId     UNIQUEIDENTIFIER NULL,
        SnapshotDate    DATE NOT NULL,
        CustomerName    NVARCHAR(255) NOT NULL,
        ModelName       NVARCHAR(100) NOT NULL,
        ModelVersion    NVARCHAR(100) NOT NULL,
        ChurnProb       DECIMAL(18,15) NOT NULL,
        RiskScore       DECIMAL(9,4) NULL,
        SourceFileName  NVARCHAR(500) NULL,
        LoadedAt        DATETIME2(0) NOT NULL CONSTRAINT DF_ChurnScoreRaw_LoadedAt DEFAULT SYSUTCDATETIME()
    );

    CREATE INDEX IX_ChurnScoreRaw_Batch ON stg.ChurnScoreRaw (LoadBatchId, SnapshotDate);
END;
GO

/* ============================================================
   3. WAREHOUSE TABLES
   ============================================================ */

IF OBJECT_ID(N'dw.DimDate', N'U') IS NULL
BEGIN
    CREATE TABLE dw.DimDate
    (
        DateKey      INT NOT NULL CONSTRAINT PK_DimDate PRIMARY KEY,
        FullDate     DATE NOT NULL CONSTRAINT UQ_DimDate_FullDate UNIQUE,
        DayOfMonth   TINYINT NOT NULL,
        MonthNumber  TINYINT NOT NULL,
        MonthName    NVARCHAR(20) NOT NULL,
        QuarterNumber TINYINT NOT NULL,
        YearNumber   SMALLINT NOT NULL,
        YearMonth    CHAR(7) NOT NULL,
        IsWeekend    BIT NOT NULL
    );
END;
GO

IF OBJECT_ID(N'dw.DimCustomer', N'U') IS NULL
BEGIN
    CREATE TABLE dw.DimCustomer
    (
        CustomerKey       INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_DimCustomer PRIMARY KEY,
        CustomerName      NVARCHAR(255) NOT NULL CONSTRAINT UQ_DimCustomer_CustomerName UNIQUE,
        CurrentSegment    NVARCHAR(100) NULL,
        FirstPurchaseDate DATE NULL,
        LastPurchaseDate  DATE NULL,
        IsActive          BIT NOT NULL CONSTRAINT DF_DimCustomer_IsActive DEFAULT 1,
        CreatedAt         DATETIME2(0) NOT NULL CONSTRAINT DF_DimCustomer_CreatedAt DEFAULT SYSUTCDATETIME(),
        UpdatedAt         DATETIME2(0) NOT NULL CONSTRAINT DF_DimCustomer_UpdatedAt DEFAULT SYSUTCDATETIME()
    );
END;
GO

IF OBJECT_ID(N'dw.DimProduct', N'U') IS NULL
BEGIN
    CREATE TABLE dw.DimProduct
    (
        ProductKey   INT IDENTITY(1,1) NOT NULL CONSTRAINT PK_DimProduct PRIMARY KEY,
        ProductCode  NVARCHAR(100) NOT NULL CONSTRAINT UQ_DimProduct_ProductCode UNIQUE,
        ProductName  NVARCHAR(500) NULL,
        UnitName     NVARCHAR(50) NULL,
        CategoryName NVARCHAR(100) NULL,
        CreatedAt    DATETIME2(0) NOT NULL CONSTRAINT DF_DimProduct_CreatedAt DEFAULT SYSUTCDATETIME(),
        UpdatedAt    DATETIME2(0) NOT NULL CONSTRAINT DF_DimProduct_UpdatedAt DEFAULT SYSUTCDATETIME()
    );
END;
GO

IF OBJECT_ID(N'dw.FactSales', N'U') IS NULL
BEGIN
    CREATE TABLE dw.FactSales
    (
        SalesKey          BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_FactSales PRIMARY KEY,
        SourceSystem      NVARCHAR(50) NOT NULL CONSTRAINT DF_FactSales_SourceSystem DEFAULT N'ExcelDataModel',
        SourceRowNumber   INT NOT NULL,
        LoadBatchId       UNIQUEIDENTIFIER NULL,
        DateKey           INT NOT NULL,
        CustomerKey       INT NOT NULL,
        ProductKey        INT NOT NULL,
        SegmentAtSale     NVARCHAR(100) NULL,
        ImportAmount      DECIMAL(18,2) NULL,
        PromoQuantity     DECIMAL(18,4) NULL,
        SalesQuantity     DECIMAL(18,4) NULL,
        ExportAmount      DECIMAL(18,2) NULL,
        Revenue           DECIMAL(18,2) NOT NULL,
        Profit            DECIMAL(18,2) NOT NULL,
        MarginPct         DECIMAL(18,8) NULL,
        TotalQuantitySold DECIMAL(18,4) NULL,
        PromoPct          DECIMAL(18,8) NULL,
        CreatedAt         DATETIME2(0) NOT NULL CONSTRAINT DF_FactSales_CreatedAt DEFAULT SYSUTCDATETIME(),
        UpdatedAt         DATETIME2(0) NOT NULL CONSTRAINT DF_FactSales_UpdatedAt DEFAULT SYSUTCDATETIME(),
        CONSTRAINT UQ_FactSales_Source UNIQUE (SourceSystem, SourceRowNumber),
        CONSTRAINT FK_FactSales_DimDate FOREIGN KEY (DateKey) REFERENCES dw.DimDate(DateKey),
        CONSTRAINT FK_FactSales_DimCustomer FOREIGN KEY (CustomerKey) REFERENCES dw.DimCustomer(CustomerKey),
        CONSTRAINT FK_FactSales_DimProduct FOREIGN KEY (ProductKey) REFERENCES dw.DimProduct(ProductKey)
    );

    CREATE INDEX IX_FactSales_CustomerDate ON dw.FactSales (CustomerKey, DateKey) INCLUDE (Revenue, Profit, PromoPct, SegmentAtSale);
    CREATE INDEX IX_FactSales_ProductDate ON dw.FactSales (ProductKey, DateKey);
END;
GO

IF OBJECT_ID(N'dw.CustomerSnapshot', N'U') IS NULL
BEGIN
    CREATE TABLE dw.CustomerSnapshot
    (
        SnapshotDate   DATE NOT NULL,
        CustomerKey    INT NOT NULL,
        Segment        NVARCHAR(100) NULL,
        Recency        INT NOT NULL,
        Frequency      INT NOT NULL,
        AOV            DECIMAL(18,4) NOT NULL,
        PromoRate      DECIMAL(18,8) NOT NULL,
        Margin         DECIMAL(18,8) NOT NULL,
        Trend          DECIMAL(18,2) NOT NULL,
        DaysSinceFirst INT NOT NULL,
        ActiveMonths   INT NOT NULL,
        ChurnLabel     BIT NULL,
        FeatureSet     NVARCHAR(100) NOT NULL CONSTRAINT DF_CustomerSnapshot_FeatureSet DEFAULT N'customer_snapshot_v1',
        LookbackDays   INT NOT NULL CONSTRAINT DF_CustomerSnapshot_LookbackDays DEFAULT 90,
        CreatedAt      DATETIME2(0) NOT NULL CONSTRAINT DF_CustomerSnapshot_CreatedAt DEFAULT SYSUTCDATETIME(),
        UpdatedAt      DATETIME2(0) NOT NULL CONSTRAINT DF_CustomerSnapshot_UpdatedAt DEFAULT SYSUTCDATETIME(),
        CONSTRAINT PK_CustomerSnapshot PRIMARY KEY (SnapshotDate, CustomerKey),
        CONSTRAINT FK_CustomerSnapshot_DimCustomer FOREIGN KEY (CustomerKey) REFERENCES dw.DimCustomer(CustomerKey)
    );
END;
GO

IF OBJECT_ID(N'dw.FactChurnScore', N'U') IS NULL
BEGIN
    CREATE TABLE dw.FactChurnScore
    (
        ChurnScoreKey BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_FactChurnScore PRIMARY KEY,
        SnapshotDate  DATE NOT NULL,
        CustomerKey   INT NOT NULL,
        ModelName     NVARCHAR(100) NOT NULL,
        ModelVersion  NVARCHAR(100) NOT NULL,
        ChurnProb     DECIMAL(18,15) NOT NULL,
        RiskScore     DECIMAL(9,4) NULL,
        RiskLevel     NVARCHAR(50) NULL,
        SalesAction   NVARCHAR(100) NULL,
        LoadBatchId   UNIQUEIDENTIFIER NULL,
        CreatedAt     DATETIME2(0) NOT NULL CONSTRAINT DF_FactChurnScore_CreatedAt DEFAULT SYSUTCDATETIME(),
        UpdatedAt     DATETIME2(0) NOT NULL CONSTRAINT DF_FactChurnScore_UpdatedAt DEFAULT SYSUTCDATETIME(),
        CONSTRAINT UQ_FactChurnScore UNIQUE (SnapshotDate, CustomerKey, ModelName, ModelVersion),
        CONSTRAINT FK_FactChurnScore_DimCustomer FOREIGN KEY (CustomerKey) REFERENCES dw.DimCustomer(CustomerKey)
    );

    CREATE INDEX IX_FactChurnScore_SnapshotRisk ON dw.FactChurnScore (SnapshotDate, RiskLevel, ChurnProb DESC);
END;
GO

/* ============================================================
   4. ETL PROCEDURES
   ============================================================ */

CREATE OR ALTER PROCEDURE dw.sp_ETL_LoadDimDate
    @StartDate DATE,
    @EndDate DATE
AS
BEGIN
    SET NOCOUNT ON;

    IF @StartDate IS NULL OR @EndDate IS NULL OR @StartDate > @EndDate
        THROW 50001, 'Invalid date range for dw.sp_ETL_LoadDimDate.', 1;

    ;WITH DateList AS
    (
        SELECT @StartDate AS FullDate
        UNION ALL
        SELECT DATEADD(DAY, 1, FullDate)
        FROM DateList
        WHERE FullDate < @EndDate
    )
    MERGE dw.DimDate AS tgt
    USING
    (
        SELECT
            CONVERT(INT, CONVERT(CHAR(8), FullDate, 112)) AS DateKey,
            FullDate,
            DATEPART(DAY, FullDate) AS DayOfMonth,
            DATEPART(MONTH, FullDate) AS MonthNumber,
            DATENAME(MONTH, FullDate) AS MonthName,
            DATEPART(QUARTER, FullDate) AS QuarterNumber,
            DATEPART(YEAR, FullDate) AS YearNumber,
            CONVERT(CHAR(7), FullDate, 120) AS YearMonth,
            CASE WHEN DATEDIFF(DAY, CONVERT(DATE, '19000101'), FullDate) % 7 IN (5, 6) THEN 1 ELSE 0 END AS IsWeekend
        FROM DateList
    ) AS src
    ON tgt.DateKey = src.DateKey
    WHEN MATCHED THEN
        UPDATE SET FullDate = src.FullDate, DayOfMonth = src.DayOfMonth, MonthNumber = src.MonthNumber,
                   MonthName = src.MonthName, QuarterNumber = src.QuarterNumber, YearNumber = src.YearNumber,
                   YearMonth = src.YearMonth, IsWeekend = src.IsWeekend
    WHEN NOT MATCHED THEN
        INSERT (DateKey, FullDate, DayOfMonth, MonthNumber, MonthName, QuarterNumber, YearNumber, YearMonth, IsWeekend)
        VALUES (src.DateKey, src.FullDate, src.DayOfMonth, src.MonthNumber, src.MonthName, src.QuarterNumber,
                src.YearNumber, src.YearMonth, src.IsWeekend)
    OPTION (MAXRECURSION 0);
END;
GO

CREATE OR ALTER PROCEDURE dw.sp_ETL_LoadDimensions
    @LoadBatchId UNIQUEIDENTIFIER = NULL
AS
BEGIN
    SET NOCOUNT ON;

    ;WITH SourceRows AS
    (
        SELECT *
        FROM stg.SalesRaw
        WHERE @LoadBatchId IS NULL OR LoadBatchId = @LoadBatchId
    ),
    CustomerAgg AS
    (
        SELECT
            CustomerName,
            SegmentName,
            MIN(SaleDate) AS FirstPurchaseDate,
            MAX(SaleDate) AS LastPurchaseDate,
            ROW_NUMBER() OVER (PARTITION BY CustomerName ORDER BY MAX(SaleDate) DESC) AS rn
        FROM SourceRows
        GROUP BY CustomerName, SegmentName
    )
    MERGE dw.DimCustomer AS tgt
    USING
    (
        SELECT CustomerName, SegmentName, FirstPurchaseDate, LastPurchaseDate
        FROM CustomerAgg
        WHERE rn = 1
    ) AS src
    ON tgt.CustomerName = src.CustomerName
    WHEN MATCHED THEN
        UPDATE SET CurrentSegment = src.SegmentName,
                   FirstPurchaseDate = CASE
                        WHEN tgt.FirstPurchaseDate IS NULL OR src.FirstPurchaseDate < tgt.FirstPurchaseDate THEN src.FirstPurchaseDate
                        ELSE tgt.FirstPurchaseDate END,
                   LastPurchaseDate = CASE
                        WHEN tgt.LastPurchaseDate IS NULL OR src.LastPurchaseDate > tgt.LastPurchaseDate THEN src.LastPurchaseDate
                        ELSE tgt.LastPurchaseDate END,
                   UpdatedAt = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
        INSERT (CustomerName, CurrentSegment, FirstPurchaseDate, LastPurchaseDate)
        VALUES (src.CustomerName, src.SegmentName, src.FirstPurchaseDate, src.LastPurchaseDate);

    ;WITH SourceRows AS
    (
        SELECT *
        FROM stg.SalesRaw
        WHERE @LoadBatchId IS NULL OR LoadBatchId = @LoadBatchId
    ),
    ProductAgg AS
    (
        SELECT
            ProductCode,
            ProductName,
            UnitName,
            CategoryName,
            ROW_NUMBER() OVER (PARTITION BY ProductCode ORDER BY COUNT(*) DESC, MAX(SaleDate) DESC) AS rn
        FROM SourceRows
        GROUP BY ProductCode, ProductName, UnitName, CategoryName
    )
    MERGE dw.DimProduct AS tgt
    USING
    (
        SELECT ProductCode, ProductName, UnitName, CategoryName
        FROM ProductAgg
        WHERE rn = 1
    ) AS src
    ON tgt.ProductCode = src.ProductCode
    WHEN MATCHED THEN
        UPDATE SET ProductName = src.ProductName, UnitName = src.UnitName, CategoryName = src.CategoryName,
                   UpdatedAt = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
        INSERT (ProductCode, ProductName, UnitName, CategoryName)
        VALUES (src.ProductCode, src.ProductName, src.UnitName, src.CategoryName);
END;
GO

CREATE OR ALTER PROCEDURE dw.sp_ETL_LoadFactSales
    @LoadBatchId UNIQUEIDENTIFIER = NULL
AS
BEGIN
    SET NOCOUNT ON;

    ;WITH SourceRows AS
    (
        SELECT *
        FROM stg.SalesRaw
        WHERE @LoadBatchId IS NULL OR LoadBatchId = @LoadBatchId
    )
    MERGE dw.FactSales AS tgt
    USING
    (
        SELECT
            N'ExcelDataModel' AS SourceSystem,
            s.SourceRowNumber,
            s.LoadBatchId,
            CONVERT(INT, CONVERT(CHAR(8), s.SaleDate, 112)) AS DateKey,
            c.CustomerKey,
            p.ProductKey,
            s.SegmentName,
            s.ImportAmount,
            s.PromoQuantity,
            s.SalesQuantity,
            s.ExportAmount,
            s.Revenue,
            s.Profit,
            s.MarginPct,
            s.TotalQuantitySold,
            s.PromoPct
        FROM SourceRows s
        INNER JOIN dw.DimCustomer c ON c.CustomerName = s.CustomerName
        INNER JOIN dw.DimProduct p ON p.ProductCode = s.ProductCode
    ) AS src
    ON tgt.SourceSystem = src.SourceSystem AND tgt.SourceRowNumber = src.SourceRowNumber
    WHEN MATCHED THEN
        UPDATE SET LoadBatchId = src.LoadBatchId, DateKey = src.DateKey, CustomerKey = src.CustomerKey,
                   ProductKey = src.ProductKey, SegmentAtSale = src.SegmentName, ImportAmount = src.ImportAmount,
                   PromoQuantity = src.PromoQuantity, SalesQuantity = src.SalesQuantity, ExportAmount = src.ExportAmount,
                   Revenue = src.Revenue, Profit = src.Profit, MarginPct = src.MarginPct,
                   TotalQuantitySold = src.TotalQuantitySold, PromoPct = src.PromoPct,
                   UpdatedAt = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
        INSERT (SourceSystem, SourceRowNumber, LoadBatchId, DateKey, CustomerKey, ProductKey, SegmentAtSale,
                ImportAmount, PromoQuantity, SalesQuantity, ExportAmount, Revenue, Profit, MarginPct,
                TotalQuantitySold, PromoPct)
        VALUES (src.SourceSystem, src.SourceRowNumber, src.LoadBatchId, src.DateKey, src.CustomerKey, src.ProductKey,
                src.SegmentName, src.ImportAmount, src.PromoQuantity, src.SalesQuantity, src.ExportAmount,
                src.Revenue, src.Profit, src.MarginPct, src.TotalQuantitySold, src.PromoPct);
END;
GO

CREATE OR ALTER PROCEDURE dw.sp_ETL_RunAll
    @LoadBatchId UNIQUEIDENTIFIER = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @StartDate DATE, @EndDate DATE;

    SELECT @StartDate = MIN(SaleDate), @EndDate = MAX(SaleDate)
    FROM stg.SalesRaw
    WHERE @LoadBatchId IS NULL OR LoadBatchId = @LoadBatchId;

    IF @StartDate IS NULL
        THROW 50002, 'No rows found in stg.SalesRaw for the requested batch.', 1;

    EXEC dw.sp_ETL_LoadDimDate @StartDate = @StartDate, @EndDate = @EndDate;
    EXEC dw.sp_ETL_LoadDimensions @LoadBatchId = @LoadBatchId;
    EXEC dw.sp_ETL_LoadFactSales @LoadBatchId = @LoadBatchId;
END;
GO

/* ============================================================
   5. FEATURE ENGINEERING
   ============================================================ */

CREATE OR ALTER PROCEDURE dw.sp_FE_BuildCustomerSnapshot
    @SnapshotDate DATE,
    @LookbackDays INT = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @LookbackDays IS NULL
        SELECT @LookbackDays = TRY_CONVERT(INT, ParameterValue)
        FROM cfg.DwParameter
        WHERE ParameterName = N'LookbackDays';

    SET @LookbackDays = ISNULL(@LookbackDays, 90);

    ;WITH SalesWindow AS
    (
        SELECT
            fs.CustomerKey,
            dd.FullDate AS SaleDate,
            fs.Revenue,
            fs.Profit,
            ISNULL(fs.PromoPct, 0) AS PromoPct,
            fs.SegmentAtSale,
            fs.SalesKey
        FROM dw.FactSales fs
        INNER JOIN dw.DimDate dd ON dd.DateKey = fs.DateKey
        WHERE dd.FullDate > DATEADD(DAY, -@LookbackDays, @SnapshotDate)
          AND dd.FullDate <= @SnapshotDate
    ),
    SegmentAsOf AS
    (
        SELECT CustomerKey, SegmentAtSale AS Segment,
               ROW_NUMBER() OVER (PARTITION BY CustomerKey ORDER BY SaleDate DESC, SalesKey DESC) AS rn
        FROM SalesWindow
    ),
    FeatureAgg AS
    (
        SELECT
            sw.CustomerKey,
            DATEDIFF(DAY, MAX(sw.SaleDate), @SnapshotDate) AS Recency,
            COUNT(*) AS Frequency,
            CAST(SUM(sw.Revenue) / NULLIF(COUNT(*), 0) AS DECIMAL(18,4)) AS AOV,
            CAST(AVG(sw.PromoPct) AS DECIMAL(18,8)) AS PromoRate,
            CAST(ISNULL(SUM(sw.Profit) / NULLIF(SUM(sw.Revenue), 0), 0) AS DECIMAL(18,8)) AS Margin,
            CAST(
                SUM(CASE WHEN sw.SaleDate > DATEADD(DAY, -30, @SnapshotDate) THEN sw.Revenue ELSE 0 END)
                - SUM(CASE WHEN sw.SaleDate <= DATEADD(DAY, -30, @SnapshotDate)
                             AND sw.SaleDate > DATEADD(DAY, -60, @SnapshotDate) THEN sw.Revenue ELSE 0 END)
                AS DECIMAL(18,2)
            ) AS Trend,
            DATEDIFF(DAY, MIN(sw.SaleDate), @SnapshotDate) AS DaysSinceFirst,
            COUNT(DISTINCT CONVERT(CHAR(7), sw.SaleDate, 120)) AS ActiveMonths
        FROM SalesWindow sw
        GROUP BY sw.CustomerKey
    )
    MERGE dw.CustomerSnapshot AS tgt
    USING
    (
        SELECT
            f.CustomerKey,
            s.Segment,
            f.Recency,
            f.Frequency,
            f.AOV,
            f.PromoRate,
            f.Margin,
            f.Trend,
            f.DaysSinceFirst,
            f.ActiveMonths
        FROM FeatureAgg f
        LEFT JOIN SegmentAsOf s ON s.CustomerKey = f.CustomerKey AND s.rn = 1
    ) AS src
    ON tgt.SnapshotDate = @SnapshotDate AND tgt.CustomerKey = src.CustomerKey
    WHEN MATCHED THEN
        UPDATE SET Segment = src.Segment, Recency = src.Recency, Frequency = src.Frequency,
                   AOV = src.AOV, PromoRate = src.PromoRate, Margin = src.Margin, Trend = src.Trend,
                   DaysSinceFirst = src.DaysSinceFirst, ActiveMonths = src.ActiveMonths,
                   LookbackDays = @LookbackDays, UpdatedAt = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
        INSERT (SnapshotDate, CustomerKey, Segment, Recency, Frequency, AOV, PromoRate, Margin,
                Trend, DaysSinceFirst, ActiveMonths, LookbackDays)
        VALUES (@SnapshotDate, src.CustomerKey, src.Segment, src.Recency, src.Frequency, src.AOV,
                src.PromoRate, src.Margin, src.Trend, src.DaysSinceFirst, src.ActiveMonths, @LookbackDays);
END;
GO

CREATE OR ALTER PROCEDURE dw.sp_FE_BuildChurnLabel
    @SnapshotDate DATE,
    @LabelStartDate DATE = NULL,
    @LabelEndDate DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;

    SET @LabelStartDate = ISNULL(@LabelStartDate, DATEADD(DAY, 1, @SnapshotDate));
    SET @LabelEndDate = ISNULL(@LabelEndDate, EOMONTH(@LabelStartDate));

    UPDATE cs
    SET ChurnLabel = CASE WHEN EXISTS
        (
            SELECT 1
            FROM dw.FactSales fs
            INNER JOIN dw.DimDate dd ON dd.DateKey = fs.DateKey
            WHERE fs.CustomerKey = cs.CustomerKey
              AND dd.FullDate >= @LabelStartDate
              AND dd.FullDate <= @LabelEndDate
        )
        THEN 0 ELSE 1 END,
        UpdatedAt = SYSUTCDATETIME()
    FROM dw.CustomerSnapshot cs
    WHERE cs.SnapshotDate = @SnapshotDate;
END;
GO

CREATE OR ALTER VIEW dw.vw_ModelInput
AS
SELECT
    cs.SnapshotDate,
    dc.CustomerName,
    cs.Segment,
    cs.Recency,
    cs.Frequency,
    cs.AOV,
    cs.PromoRate,
    cs.Margin,
    cs.Trend,
    cs.DaysSinceFirst,
    cs.ActiveMonths,
    cs.ChurnLabel
FROM dw.CustomerSnapshot cs
INNER JOIN dw.DimCustomer dc ON dc.CustomerKey = cs.CustomerKey;
GO

/* ============================================================
   6. SCORING
   ============================================================ */

CREATE OR ALTER PROCEDURE dw.sp_Score_ApplyBusinessRules
    @SnapshotDate DATE,
    @ModelName NVARCHAR(100) = NULL,
    @ModelVersion NVARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Urgent DECIMAL(18,15) = (SELECT TRY_CONVERT(DECIMAL(18,15), ParameterValue) FROM cfg.DwParameter WHERE ParameterName = N'UrgentThreshold');
    DECLARE @High DECIMAL(18,15) = (SELECT TRY_CONVERT(DECIMAL(18,15), ParameterValue) FROM cfg.DwParameter WHERE ParameterName = N'HighThreshold');
    DECLARE @Medium DECIMAL(18,15) = (SELECT TRY_CONVERT(DECIMAL(18,15), ParameterValue) FROM cfg.DwParameter WHERE ParameterName = N'MediumThreshold');

    SET @Urgent = ISNULL(@Urgent, 0.538023093024565);
    SET @High = ISNULL(@High, 0.35);
    SET @Medium = ISNULL(@Medium, 0.20);

    UPDATE fcs
    SET RiskScore = ISNULL(fcs.RiskScore, CAST(fcs.ChurnProb * 10 AS DECIMAL(9,4))),
        RiskLevel = CASE
            WHEN fcs.ChurnProb >= @Urgent THEN N'Khẩn cấp'
            WHEN fcs.ChurnProb >= @High THEN N'Cao'
            WHEN fcs.ChurnProb >= @Medium THEN N'Trung bình'
            ELSE N'Thấp'
        END,
        SalesAction = CASE
            WHEN fcs.ChurnProb >= @Urgent AND cs.Segment = N'Khách hàng VIP' THEN N'Gặp mặt trực tiếp'
            WHEN fcs.ChurnProb >= @Urgent OR cs.Recency > 60 THEN N'Gọi ngay'
            WHEN fcs.ChurnProb >= @High OR cs.Recency > 30 THEN N'Zalo offer'
            ELSE N'Theo dõi định kỳ'
        END,
        UpdatedAt = SYSUTCDATETIME()
    FROM dw.FactChurnScore fcs
    LEFT JOIN dw.CustomerSnapshot cs
        ON cs.CustomerKey = fcs.CustomerKey AND cs.SnapshotDate = fcs.SnapshotDate
    WHERE fcs.SnapshotDate = @SnapshotDate
      AND (@ModelName IS NULL OR fcs.ModelName = @ModelName)
      AND (@ModelVersion IS NULL OR fcs.ModelVersion = @ModelVersion);
END;
GO

CREATE OR ALTER PROCEDURE dw.sp_Score_LoadFromStaging
    @SnapshotDate DATE,
    @LoadBatchId UNIQUEIDENTIFIER = NULL,
    @ModelName NVARCHAR(100) = NULL,
    @ModelVersion NVARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @ModelName IS NULL
        SELECT @ModelName = ParameterValue FROM cfg.DwParameter WHERE ParameterName = N'ActiveModelName';
    IF @ModelVersion IS NULL
        SELECT @ModelVersion = ParameterValue FROM cfg.DwParameter WHERE ParameterName = N'ActiveModelVersion';

    MERGE dw.FactChurnScore AS tgt
    USING
    (
        SELECT
            csr.SnapshotDate,
            dc.CustomerKey,
            csr.ModelName,
            csr.ModelVersion,
            csr.ChurnProb,
            csr.RiskScore,
            csr.LoadBatchId
        FROM stg.ChurnScoreRaw csr
        INNER JOIN dw.DimCustomer dc ON dc.CustomerName = csr.CustomerName
        WHERE csr.SnapshotDate = @SnapshotDate
          AND (@LoadBatchId IS NULL OR csr.LoadBatchId = @LoadBatchId)
          AND (@ModelName IS NULL OR csr.ModelName = @ModelName)
          AND (@ModelVersion IS NULL OR csr.ModelVersion = @ModelVersion)
    ) AS src
    ON tgt.SnapshotDate = src.SnapshotDate
       AND tgt.CustomerKey = src.CustomerKey
       AND tgt.ModelName = src.ModelName
       AND tgt.ModelVersion = src.ModelVersion
    WHEN MATCHED THEN
        UPDATE SET ChurnProb = src.ChurnProb, RiskScore = src.RiskScore, LoadBatchId = src.LoadBatchId,
                   UpdatedAt = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
        INSERT (SnapshotDate, CustomerKey, ModelName, ModelVersion, ChurnProb, RiskScore, LoadBatchId)
        VALUES (src.SnapshotDate, src.CustomerKey, src.ModelName, src.ModelVersion, src.ChurnProb, src.RiskScore, src.LoadBatchId);

    EXEC dw.sp_Score_ApplyBusinessRules @SnapshotDate = @SnapshotDate, @ModelName = @ModelName, @ModelVersion = @ModelVersion;
END;
GO

CREATE OR ALTER PROCEDURE dw.sp_Score_UpsertChurnScore
    @SnapshotDate DATE,
    @CustomerName NVARCHAR(255),
    @ChurnProb DECIMAL(18,15),
    @RiskScore DECIMAL(9,4) = NULL,
    @ModelName NVARCHAR(100) = NULL,
    @ModelVersion NVARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @ModelName IS NULL
        SELECT @ModelName = ParameterValue FROM cfg.DwParameter WHERE ParameterName = N'ActiveModelName';
    IF @ModelVersion IS NULL
        SELECT @ModelVersion = ParameterValue FROM cfg.DwParameter WHERE ParameterName = N'ActiveModelVersion';

    DECLARE @CustomerKey INT;
    SELECT @CustomerKey = CustomerKey FROM dw.DimCustomer WHERE CustomerName = @CustomerName;

    IF @CustomerKey IS NULL
        THROW 50003, 'CustomerName not found in dw.DimCustomer.', 1;

    MERGE dw.FactChurnScore AS tgt
    USING
    (
        SELECT @SnapshotDate AS SnapshotDate, @CustomerKey AS CustomerKey, @ModelName AS ModelName,
               @ModelVersion AS ModelVersion, @ChurnProb AS ChurnProb, @RiskScore AS RiskScore
    ) AS src
    ON tgt.SnapshotDate = src.SnapshotDate
       AND tgt.CustomerKey = src.CustomerKey
       AND tgt.ModelName = src.ModelName
       AND tgt.ModelVersion = src.ModelVersion
    WHEN MATCHED THEN
        UPDATE SET ChurnProb = src.ChurnProb, RiskScore = src.RiskScore, UpdatedAt = SYSUTCDATETIME()
    WHEN NOT MATCHED THEN
        INSERT (SnapshotDate, CustomerKey, ModelName, ModelVersion, ChurnProb, RiskScore)
        VALUES (src.SnapshotDate, src.CustomerKey, src.ModelName, src.ModelVersion, src.ChurnProb, src.RiskScore);

    EXEC dw.sp_Score_ApplyBusinessRules @SnapshotDate = @SnapshotDate, @ModelName = @ModelName, @ModelVersion = @ModelVersion;
END;
GO

/* ============================================================
   7. REPORTING
   ============================================================ */

CREATE OR ALTER PROCEDURE dw.sp_Report_ChurnList
    @SnapshotDate DATE,
    @ModelName NVARCHAR(100) = NULL,
    @ModelVersion NVARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @ModelName IS NULL
        SELECT @ModelName = ParameterValue FROM cfg.DwParameter WHERE ParameterName = N'ActiveModelName';
    IF @ModelVersion IS NULL
        SELECT @ModelVersion = ParameterValue FROM cfg.DwParameter WHERE ParameterName = N'ActiveModelVersion';

    SELECT
        dc.CustomerName AS [Khách hàng],
        cs.Segment,
        cs.Recency,
        cs.Frequency,
        cs.AOV,
        cs.Margin,
        cs.Trend,
        cs.ActiveMonths,
        fcs.RiskScore,
        fcs.ChurnProb,
        fcs.RiskLevel AS [Mức độ],
        fcs.SalesAction AS [Hành động]
    FROM dw.FactChurnScore fcs
    INNER JOIN dw.DimCustomer dc ON dc.CustomerKey = fcs.CustomerKey
    LEFT JOIN dw.CustomerSnapshot cs ON cs.CustomerKey = fcs.CustomerKey AND cs.SnapshotDate = fcs.SnapshotDate
    WHERE fcs.SnapshotDate = @SnapshotDate
      AND fcs.ModelName = @ModelName
      AND fcs.ModelVersion = @ModelVersion
    ORDER BY fcs.ChurnProb DESC, cs.Recency DESC;
END;
GO

CREATE OR ALTER PROCEDURE dw.sp_Report_ChurnBySegment
    @SnapshotDate DATE,
    @ModelName NVARCHAR(100) = NULL,
    @ModelVersion NVARCHAR(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @ModelName IS NULL
        SELECT @ModelName = ParameterValue FROM cfg.DwParameter WHERE ParameterName = N'ActiveModelName';
    IF @ModelVersion IS NULL
        SELECT @ModelVersion = ParameterValue FROM cfg.DwParameter WHERE ParameterName = N'ActiveModelVersion';

    SELECT
        cs.Segment,
        COUNT(*) AS customers,
        CAST(AVG(fcs.ChurnProb) AS DECIMAL(18,6)) AS mean_churn_prob,
        SUM(CASE WHEN fcs.RiskLevel = N'Khẩn cấp' THEN 1 ELSE 0 END) AS urgent_customers,
        SUM(CASE WHEN fcs.RiskLevel IN (N'Cao', N'Khẩn cấp') THEN 1 ELSE 0 END) AS high_or_urgent,
        CAST(AVG(CAST(cs.Recency AS FLOAT)) AS DECIMAL(18,2)) AS avg_recency,
        CAST(AVG(cs.AOV) AS DECIMAL(18,2)) AS avg_aov,
        CAST(AVG(cs.Margin) AS DECIMAL(18,6)) AS avg_margin
    FROM dw.FactChurnScore fcs
    LEFT JOIN dw.CustomerSnapshot cs ON cs.CustomerKey = fcs.CustomerKey AND cs.SnapshotDate = fcs.SnapshotDate
    WHERE fcs.SnapshotDate = @SnapshotDate
      AND fcs.ModelName = @ModelName
      AND fcs.ModelVersion = @ModelVersion
    GROUP BY cs.Segment
    ORDER BY mean_churn_prob DESC;
END;
GO

CREATE OR ALTER PROCEDURE dw.sp_Report_RFM_Summary
    @SnapshotDate DATE
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        Segment,
        COUNT(*) AS customers,
        CAST(AVG(CAST(Recency AS FLOAT)) AS DECIMAL(18,2)) AS avg_recency,
        CAST(AVG(CAST(Frequency AS FLOAT)) AS DECIMAL(18,2)) AS avg_frequency,
        CAST(AVG(AOV) AS DECIMAL(18,2)) AS avg_aov,
        CAST(AVG(Margin) AS DECIMAL(18,6)) AS avg_margin,
        CAST(AVG(PromoRate) AS DECIMAL(18,6)) AS avg_promo_rate
    FROM dw.CustomerSnapshot
    WHERE SnapshotDate = @SnapshotDate
    GROUP BY Segment
    ORDER BY avg_recency DESC;
END;
GO

CREATE OR ALTER PROCEDURE dw.sp_Report_MonthlySales
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        dd.YearNumber,
        dd.MonthNumber,
        dd.YearMonth,
        COUNT(*) AS transaction_count,
        COUNT(DISTINCT fs.CustomerKey) AS customer_count,
        SUM(fs.Revenue) AS revenue,
        SUM(fs.Profit) AS profit,
        CAST(SUM(fs.Profit) / NULLIF(SUM(fs.Revenue), 0) AS DECIMAL(18,6)) AS margin
    FROM dw.FactSales fs
    INNER JOIN dw.DimDate dd ON dd.DateKey = fs.DateKey
    GROUP BY dd.YearNumber, dd.MonthNumber, dd.YearMonth
    ORDER BY dd.YearNumber, dd.MonthNumber;
END;
GO

CREATE OR ALTER PROCEDURE dw.sp_Report_TopCustomers
    @TopN INT = 20
AS
BEGIN
    SET NOCOUNT ON;

    SELECT TOP (@TopN)
        dc.CustomerName,
        dc.CurrentSegment,
        COUNT(*) AS transaction_count,
        SUM(fs.Revenue) AS revenue,
        SUM(fs.Profit) AS profit,
        CAST(SUM(fs.Profit) / NULLIF(SUM(fs.Revenue), 0) AS DECIMAL(18,6)) AS margin
    FROM dw.FactSales fs
    INNER JOIN dw.DimCustomer dc ON dc.CustomerKey = fs.CustomerKey
    GROUP BY dc.CustomerName, dc.CurrentSegment
    ORDER BY SUM(fs.Revenue) DESC;
END;
GO

CREATE OR ALTER PROCEDURE dw.sp_Report_Reconciliation
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        'stg.SalesRaw' AS source_name,
        COUNT(*) AS row_count,
        SUM(Revenue) AS revenue,
        SUM(Profit) AS profit
    FROM stg.SalesRaw
    UNION ALL
    SELECT
        'dw.FactSales' AS source_name,
        COUNT(*) AS row_count,
        SUM(Revenue) AS revenue,
        SUM(Profit) AS profit
    FROM dw.FactSales;
END;
GO
