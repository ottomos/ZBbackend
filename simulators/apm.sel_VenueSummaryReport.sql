SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*****************************************************************
 KT  — Venue x Symbol distribution & trade success
******************************************************************/
CREATE OR ALTER PROCEDURE apm.sel_VenueSummaryReport_KT
(
    @Period     NVARCHAR(10),
    @StartDate  DATE = NULL,
    @EndDate    DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET DATEFIRST 6;

    -- Ornek Venue listesi
    DECLARE @Venues TABLE (Venue NVARCHAR(50));
    INSERT INTO @Venues (Venue) VALUES
        ('Integral'), ('Tradair'), ('360T'), ('FXAll');

    -- Ornek Symbol listesi
    DECLARE @Symbols TABLE (Symbol NVARCHAR(20));
    INSERT INTO @Symbols (Symbol) VALUES
        ('EUR/USD'), ('GBP/USD'), ('USD/JPY'), ('USD/CHF'),
        ('AUD/USD'), ('USD/CAD'), ('NZD/USD'), ('XAU/USD'),
        ('EUR/GBP'), ('EUR/JPY');

    ;WITH RandomData AS
    (
        SELECT
            v.Venue,
            s.Symbol,
            CAST(
                ROUND(
                    ABS(CHECKSUM(NEWID())) % 5000000
                    + (ABS(CHECKSUM(NEWID())) % 100) / 100.0
                , 2)
            AS DECIMAL(18,2)) AS ExecutionAmountUSD,
            (ABS(CHECKSUM(NEWID())) % 1000) + 1 AS ExecutionCount,
            (ABS(CHECKSUM(NEWID())) % 201)      AS RejectCount
        FROM @Venues v
        CROSS JOIN @Symbols s
    )
    SELECT
        Venue,
        Symbol,
        ExecutionAmountUSD,
        ExecutionCount,
        RejectCount
    FROM RandomData
    ORDER BY ExecutionAmountUSD DESC;
END
GO

/*****************************************************************
 KFH — Venue x Symbol distribution & trade success
******************************************************************/
CREATE OR ALTER PROCEDURE apm.sel_VenueSummaryReport_KFH
(
    @Period     NVARCHAR(10),
    @StartDate  DATE = NULL,
    @EndDate    DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET DATEFIRST 6;

    DECLARE @Venues TABLE (Venue NVARCHAR(50));
    INSERT INTO @Venues (Venue) VALUES
        ('Integral'), ('Tradair'), ('360T'), ('FXAll');

    DECLARE @Symbols TABLE (Symbol NVARCHAR(20));
    INSERT INTO @Symbols (Symbol) VALUES
        ('EUR/USD'), ('GBP/USD'), ('USD/JPY'), ('USD/CHF'),
        ('AUD/USD'), ('USD/CAD'), ('NZD/USD'), ('XAU/USD'),
        ('EUR/GBP'), ('EUR/JPY');

    ;WITH RandomData AS
    (
        SELECT
            v.Venue,
            s.Symbol,
            CAST(
                ROUND(
                    ABS(CHECKSUM(NEWID())) % 5000000
                    + (ABS(CHECKSUM(NEWID())) % 100) / 100.0
                , 2)
            AS DECIMAL(18,2)) AS ExecutionAmountUSD,
            (ABS(CHECKSUM(NEWID())) % 1000) + 1 AS ExecutionCount,
            (ABS(CHECKSUM(NEWID())) % 201)      AS RejectCount
        FROM @Venues v
        CROSS JOIN @Symbols s
    )
    SELECT
        Venue,
        Symbol,
        ExecutionAmountUSD,
        ExecutionCount,
        RejectCount
    FROM RandomData
    ORDER BY ExecutionAmountUSD DESC;
END
GO

/*****************************************************************
 AUB — Venue x Symbol distribution & trade success
******************************************************************/
CREATE OR ALTER PROCEDURE apm.sel_VenueSummaryReport_AUB
(
    @Period     NVARCHAR(10),
    @StartDate  DATE = NULL,
    @EndDate    DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET DATEFIRST 6;

    DECLARE @Venues TABLE (Venue NVARCHAR(50));
    INSERT INTO @Venues (Venue) VALUES
        ('Integral'), ('Tradair'), ('360T'), ('FXAll');

    DECLARE @Symbols TABLE (Symbol NVARCHAR(20));
    INSERT INTO @Symbols (Symbol) VALUES
        ('EUR/USD'), ('GBP/USD'), ('USD/JPY'), ('USD/CHF'),
        ('AUD/USD'), ('USD/CAD'), ('NZD/USD'), ('XAU/USD'),
        ('EUR/GBP'), ('EUR/JPY');

    ;WITH RandomData AS
    (
        SELECT
            v.Venue,
            s.Symbol,
            CAST(
                ROUND(
                    ABS(CHECKSUM(NEWID())) % 5000000
                    + (ABS(CHECKSUM(NEWID())) % 100) / 100.0
                , 2)
            AS DECIMAL(18,2)) AS ExecutionAmountUSD,
            (ABS(CHECKSUM(NEWID())) % 1000) + 1 AS ExecutionCount,
            (ABS(CHECKSUM(NEWID())) % 201)      AS RejectCount
        FROM @Venues v
        CROSS JOIN @Symbols s
    )
    SELECT
        Venue,
        Symbol,
        ExecutionAmountUSD,
        ExecutionCount,
        RejectCount
    FROM RandomData
    ORDER BY ExecutionAmountUSD DESC;
END
GO
