SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*****************************************************************
 KT
******************************************************************/
CREATE OR ALTER PROCEDURE apm.sel_InterbankExecutionSummaryReport_KT
(
    @Period NVARCHAR(10),
    @StartDate  DATE = NULL,
    @EndDate    DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Prefix NVARCHAR(10);

    IF      @Period = 'TODAY'  SET @Prefix = '11';
    ELSE IF @Period = 'WTD'    SET @Prefix = '12';
    ELSE IF @Period = 'MTD'    SET @Prefix = '13';
    ELSE IF @Period = 'QTD'    SET @Prefix = '14';
    ELSE IF @Period = 'YTD'    SET @Prefix = '15';
    ELSE IF @Period = 'CUSTOM'
        SET @Prefix = '16' + CAST(DAY(@StartDate) AS NVARCHAR)
                           + CAST(DAY(@EndDate)   AS NVARCHAR);
    ELSE
    BEGIN
        RAISERROR('Invalid Period',16,1);
        RETURN;
    END;

    DECLARE @Symbols TABLE (Symbol NVARCHAR(10));
    INSERT INTO @Symbols VALUES
    ('AUD/USD'),('EUR/USD'),('GBP/USD'),
    ('USD/CAD'),('USD/CHF'),('USD/KWD'),
    ('USD/RUB'),('USD/SAR'),('USD/TRY'),
    ('XAG/USD'),('XAU/USD'),('XPD/USD'),('XPT/USD');

    ;WITH BaseData AS
    (
        SELECT
            Symbol,
            (ABS(CHECKSUM(NEWID())) % 100000000) / 100.0 AS BaseAmount,
            (ABS(CHECKSUM(NEWID())) % 100) / 100.0       AS BuyRatio
        FROM @Symbols
    )
    SELECT
        Symbol,
        CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount AS INT) AS NVARCHAR),6) AS DECIMAL(18,2)) AS TotalAmount,
        CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount * BuyRatio AS INT) AS NVARCHAR),6) AS DECIMAL(18,2)) AS TotalBuy,
        CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount * (1 - BuyRatio) AS INT) AS NVARCHAR),6) AS DECIMAL(18,2)) AS TotalSell,
        CAST(
            CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount * BuyRatio AS INT) AS NVARCHAR),6) AS DECIMAL(18,2))
          - CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount * (1 - BuyRatio) AS INT) AS NVARCHAR),6) AS DECIMAL(18,2))
        AS DECIMAL(18,2)) AS NetAmount,
        CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount AS INT) AS NVARCHAR),6) AS DECIMAL(18,2))*1.2 AS TotalAmountUSD
    FROM BaseData
    ORDER BY Symbol;
END
GO

/*****************************************************************
 AUB
******************************************************************/
CREATE OR ALTER PROCEDURE apm.sel_InterbankExecutionSummaryReport_AUB
(
    @Period NVARCHAR(10),
    @StartDate  DATE = NULL,
    @EndDate    DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Prefix NVARCHAR(10);

    IF      @Period = 'TODAY'  SET @Prefix = '21';
    ELSE IF @Period = 'WTD'    SET @Prefix = '22';
    ELSE IF @Period = 'MTD'    SET @Prefix = '23';
    ELSE IF @Period = 'QTD'    SET @Prefix = '24';
    ELSE IF @Period = 'YTD'    SET @Prefix = '25';
    ELSE IF @Period = 'CUSTOM'
        SET @Prefix = '26' + CAST(DAY(@StartDate) AS NVARCHAR)
                           + CAST(DAY(@EndDate)   AS NVARCHAR);
    ELSE
    BEGIN
        RAISERROR('Invalid Period',16,1);
        RETURN;
    END;

    DECLARE @Symbols TABLE (Symbol NVARCHAR(10));
    INSERT INTO @Symbols VALUES
    ('AUD/USD'),('EUR/USD'),('GBP/USD'),
    ('USD/CAD'),('USD/CHF'),('USD/KWD'),
    ('USD/RUB'),('USD/SAR'),('USD/TRY'),
    ('XAG/USD'),('XAU/USD'),('XPD/USD'),('XPT/USD');

    ;WITH BaseData AS
    (
        SELECT
            Symbol,
            (ABS(CHECKSUM(NEWID())) % 100000000) / 100.0 AS BaseAmount,
            (ABS(CHECKSUM(NEWID())) % 100) / 100.0       AS BuyRatio
        FROM @Symbols
    )
    SELECT
        Symbol,
        CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount AS INT) AS NVARCHAR),6) AS DECIMAL(18,2)) AS TotalAmount,
        CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount * BuyRatio AS INT) AS NVARCHAR),6) AS DECIMAL(18,2)) AS TotalBuy,
        CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount * (1 - BuyRatio) AS INT) AS NVARCHAR),6) AS DECIMAL(18,2)) AS TotalSell,
        CAST(
            CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount * BuyRatio AS INT) AS NVARCHAR),6) AS DECIMAL(18,2))
          - CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount * (1 - BuyRatio) AS INT) AS NVARCHAR),6) AS DECIMAL(18,2))
        AS DECIMAL(18,2)) AS NetAmount,
        CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount AS INT) AS NVARCHAR),6) AS DECIMAL(18,2))*1.2 AS TotalAmountUSD
    FROM BaseData
    ORDER BY Symbol;
END
GO

/*****************************************************************
 KFH
******************************************************************/
CREATE OR ALTER PROCEDURE apm.sel_InterbankExecutionSummaryReport_KFH
(
    @Period NVARCHAR(10),
    @StartDate  DATE = NULL,
    @EndDate    DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Prefix NVARCHAR(10);

    IF      @Period = 'TODAY'  SET @Prefix = '31';
    ELSE IF @Period = 'WTD'    SET @Prefix = '32';
    ELSE IF @Period = 'MTD'    SET @Prefix = '33';
    ELSE IF @Period = 'QTD'    SET @Prefix = '34';
    ELSE IF @Period = 'YTD'    SET @Prefix = '35';
    ELSE IF @Period = 'CUSTOM'
        SET @Prefix = '36' + CAST(DAY(@StartDate) AS NVARCHAR)
                           + CAST(DAY(@EndDate)   AS NVARCHAR);
    ELSE
    BEGIN
        RAISERROR('Invalid Period',16,1);
        RETURN;
    END;

    DECLARE @Symbols TABLE (Symbol NVARCHAR(10));
    INSERT INTO @Symbols VALUES
    ('AUD/USD'),('EUR/USD'),('GBP/USD'),
    ('USD/CAD'),('USD/CHF'),('USD/KWD'),
    ('USD/RUB'),('USD/SAR'),('USD/TRY'),
    ('XAG/USD'),('XAU/USD'),('XPD/USD'),('XPT/USD');

    ;WITH BaseData AS
    (
        SELECT
            Symbol,
            (ABS(CHECKSUM(NEWID())) % 100000000) / 100.0 AS BaseAmount,
            (ABS(CHECKSUM(NEWID())) % 100) / 100.0       AS BuyRatio
        FROM @Symbols
    )
    SELECT
        Symbol,
        CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount AS INT) AS NVARCHAR),6) AS DECIMAL(18,2)) AS TotalAmount,
        CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount * BuyRatio AS INT) AS NVARCHAR),6) AS DECIMAL(18,2)) AS TotalBuy,
        CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount * (1 - BuyRatio) AS INT) AS NVARCHAR),6) AS DECIMAL(18,2)) AS TotalSell,
        CAST(
            CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount * BuyRatio AS INT) AS NVARCHAR),6) AS DECIMAL(18,2))
          - CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount * (1 - BuyRatio) AS INT) AS NVARCHAR),6) AS DECIMAL(18,2))
        AS DECIMAL(18,2)) AS NetAmount,
        CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount AS INT) AS NVARCHAR),6) AS DECIMAL(18,2))*1.2 AS TotalAmountUSD
    FROM BaseData
    ORDER BY Symbol;
END
GO





--EXEC [dbo].[sel_InterbankExecutionSummaryReport_KFH]

--    @Period = 'CUSTOM',
--    @StartDate = '2026-02-09',
--    @EndDate   = '2026-02-05';


--EXEC [dbo].[sel_InterbankExecutionSummaryReport_KT]

--    @Period = 'WTD'


--EXEC [dbo].[sel_InterbankExecutionSummaryReport_AUB]

--    @Period = 'TODAY'