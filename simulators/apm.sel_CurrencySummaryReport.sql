SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*****************************************************************
KT – Currency Summary Report SIM
******************************************************************/
CREATE OR ALTER PROCEDURE apm.sel_CurrencySummaryReport_KT
(
    @Period     NVARCHAR(10),
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

    DECLARE @Currencies TABLE (Currency NVARCHAR(3));
    INSERT INTO @Currencies VALUES
    ('AED'),('AUD'),('CAD'),('CHF'),('EUR'),('GBP'),
    ('JPY'),('NOK'),('SAR'),('SEK'),('TRY'),
    ('USD'),('XAG'),('XAU'),('XPT');

    SELECT
        Currency,
        CAST(@Prefix + RIGHT('000000' + CAST(ABS(CHECKSUM(NEWID())) % 1000000 AS NVARCHAR),6) AS DECIMAL(18,2)) AS TotalBuyAmount,
        CAST(@Prefix + RIGHT('000000' + CAST(ABS(CHECKSUM(NEWID())) % 1000000 AS NVARCHAR),6) AS DECIMAL(18,2)) AS TotalSellAmount,
        CAST(
            CAST(@Prefix + RIGHT('000000' + CAST(ABS(CHECKSUM(NEWID())) % 1000000 AS NVARCHAR),6) AS DECIMAL(18,2))
          - CAST(@Prefix + RIGHT('000000' + CAST(ABS(CHECKSUM(NEWID())) % 1000000 AS NVARCHAR),6) AS DECIMAL(18,2))
        AS DECIMAL(18,2)) AS NetAmount
    FROM @Currencies
    ORDER BY Currency;
END
GO

/*****************************************************************
AUB – Currency Summary Report SIM
******************************************************************/
CREATE OR ALTER PROCEDURE apm.sel_CurrencySummaryReport_AUB
(
    @Period     NVARCHAR(10),
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

    DECLARE @Currencies TABLE (Currency NVARCHAR(3));
    INSERT INTO @Currencies VALUES
    ('AED'),('AUD'),('CAD'),('CHF'),('EUR'),('GBP'),
    ('JPY'),('NOK'),('SAR'),('SEK'),('TRY'),
    ('USD'),('XAG'),('XAU'),('XPT');

    SELECT
        Currency,
        CAST(@Prefix + RIGHT('000000' + CAST(ABS(CHECKSUM(NEWID())) % 1000000 AS NVARCHAR),6) AS DECIMAL(18,2)) AS TotalBuyAmount,
        CAST(@Prefix + RIGHT('000000' + CAST(ABS(CHECKSUM(NEWID())) % 1000000 AS NVARCHAR),6) AS DECIMAL(18,2)) AS TotalSellAmount,
        CAST(
            CAST(@Prefix + RIGHT('000000' + CAST(ABS(CHECKSUM(NEWID())) % 1000000 AS NVARCHAR),6) AS DECIMAL(18,2))
          - CAST(@Prefix + RIGHT('000000' + CAST(ABS(CHECKSUM(NEWID())) % 1000000 AS NVARCHAR),6) AS DECIMAL(18,2))
        AS DECIMAL(18,2)) AS NetAmount
    FROM @Currencies
    ORDER BY Currency;
END
GO

/*****************************************************************
KFH – Currency Summary Report SIM
******************************************************************/
CREATE OR ALTER PROCEDURE apm.sel_CurrencySummaryReport_KFH
(
    @Period     NVARCHAR(10),
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

    DECLARE @Currencies TABLE (Currency NVARCHAR(3));
    INSERT INTO @Currencies VALUES
    ('AED'),('AUD'),('CAD'),('CHF'),('EUR'),('GBP'),
    ('JPY'),('NOK'),('SAR'),('SEK'),('TRY'),
    ('USD'),('XAG'),('XAU'),('XPT');

    SELECT
        Currency,
        CAST(@Prefix + RIGHT('000000' + CAST(ABS(CHECKSUM(NEWID())) % 1000000 AS NVARCHAR),6) AS DECIMAL(18,2)) AS TotalBuyAmount,
        CAST(@Prefix + RIGHT('000000' + CAST(ABS(CHECKSUM(NEWID())) % 1000000 AS NVARCHAR),6) AS DECIMAL(18,2)) AS TotalSellAmount,
        CAST(
            CAST(@Prefix + RIGHT('000000' + CAST(ABS(CHECKSUM(NEWID())) % 1000000 AS NVARCHAR),6) AS DECIMAL(18,2))
          - CAST(@Prefix + RIGHT('000000' + CAST(ABS(CHECKSUM(NEWID())) % 1000000 AS NVARCHAR),6) AS DECIMAL(18,2))
        AS DECIMAL(18,2)) AS NetAmount
    FROM @Currencies
    ORDER BY Currency;
END
GO

--EXEC apm.sel_CurrencySummaryReport_KT

 --   @Period = 'CUSTOM',
  -- @StartDate = '2026-02-12',
  --  @EndDate   = '2026-02-17';


--EXEC apm.sel_CurrencySummaryReport_KT

--    @Period = 'WTD'


--EXEC apm.sel_CurrencySummaryReport_AUB

  -- @Period = 'QTD'