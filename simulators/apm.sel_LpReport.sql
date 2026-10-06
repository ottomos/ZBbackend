SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*****************************************************************
 KT  — LP (counterparty) x Symbol distribution
******************************************************************/
CREATE OR ALTER PROCEDURE apm.sel_LpReport_KT
(
    @Period     NVARCHAR(10),
    @StartDate  DATE = NULL,
    @EndDate    DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET DATEFIRST 6;

    -- Ornek LP (Issuer) listesi
    DECLARE @Lps TABLE (LpName NVARCHAR(50));
    INSERT INTO @Lps (LpName) VALUES
        ('JPMorgan'), ('Goldman Sachs'), ('Morgan Stanley'),
        ('Citadel'), ('Barclays'), ('UBS'), ('Deutsche Bank'),
        ('XTX Markets'), ('Jump Trading'), ('Virtu');

    -- Ornek Symbol listesi
    DECLARE @Symbols TABLE (Symbol NVARCHAR(20));
    INSERT INTO @Symbols (Symbol) VALUES
        ('EUR/USD'), ('GBP/USD'), ('USD/JPY'), ('USD/CHF'),
        ('AUD/USD'), ('USD/CAD'), ('NZD/USD'), ('XAU/USD');

    SELECT
        l.LpName,
        s.Symbol,
        CAST(
            ROUND(
                ABS(CHECKSUM(NEWID())) % 1000000
                + (ABS(CHECKSUM(NEWID())) % 100) / 100.0
            , 2)
        AS DECIMAL(18,2)) AS Amount,
        (ABS(CHECKSUM(NEWID())) % 500) + 1 AS ExecutionCount
    FROM @Lps l
    CROSS JOIN @Symbols s
    ORDER BY Amount DESC;
END
GO

/*****************************************************************
 KFH — LP (counterparty) x Symbol distribution
******************************************************************/
CREATE OR ALTER PROCEDURE apm.sel_LpReport_KFH
(
    @Period     NVARCHAR(10),
    @StartDate  DATE = NULL,
    @EndDate    DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET DATEFIRST 6;

    DECLARE @Lps TABLE (LpName NVARCHAR(50));
    INSERT INTO @Lps (LpName) VALUES
        ('JPMorgan'), ('Goldman Sachs'), ('Morgan Stanley'),
        ('Citadel'), ('Barclays'), ('UBS'), ('Deutsche Bank'),
        ('XTX Markets'), ('Jump Trading'), ('Virtu');

    DECLARE @Symbols TABLE (Symbol NVARCHAR(20));
    INSERT INTO @Symbols (Symbol) VALUES
        ('EUR/USD'), ('GBP/USD'), ('USD/JPY'), ('USD/CHF'),
        ('AUD/USD'), ('USD/CAD'), ('NZD/USD'), ('XAU/USD');

    SELECT
        l.LpName,
        s.Symbol,
        CAST(
            ROUND(
                ABS(CHECKSUM(NEWID())) % 1000000
                + (ABS(CHECKSUM(NEWID())) % 100) / 100.0
            , 2)
        AS DECIMAL(18,2)) AS Amount,
        (ABS(CHECKSUM(NEWID())) % 500) + 1 AS ExecutionCount
    FROM @Lps l
    CROSS JOIN @Symbols s
    ORDER BY Amount DESC;
END
GO

/*****************************************************************
 AUB — LP (counterparty) x Symbol distribution
******************************************************************/
CREATE OR ALTER PROCEDURE apm.sel_LpReport_AUB
(
    @Period     NVARCHAR(10),
    @StartDate  DATE = NULL,
    @EndDate    DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET DATEFIRST 6;

    DECLARE @Lps TABLE (LpName NVARCHAR(50));
    INSERT INTO @Lps (LpName) VALUES
        ('JPMorgan'), ('Goldman Sachs'), ('Morgan Stanley'),
        ('Citadel'), ('Barclays'), ('UBS'), ('Deutsche Bank'),
        ('XTX Markets'), ('Jump Trading'), ('Virtu');

    DECLARE @Symbols TABLE (Symbol NVARCHAR(20));
    INSERT INTO @Symbols (Symbol) VALUES
        ('EUR/USD'), ('GBP/USD'), ('USD/JPY'), ('USD/CHF'),
        ('AUD/USD'), ('USD/CAD'), ('NZD/USD'), ('XAU/USD');

    SELECT
        l.LpName,
        s.Symbol,
        CAST(
            ROUND(
                ABS(CHECKSUM(NEWID())) % 1000000
                + (ABS(CHECKSUM(NEWID())) % 100) / 100.0
            , 2)
        AS DECIMAL(18,2)) AS Amount,
        (ABS(CHECKSUM(NEWID())) % 500) + 1 AS ExecutionCount
    FROM @Lps l
    CROSS JOIN @Symbols s
    ORDER BY Amount DESC;
END
GO
