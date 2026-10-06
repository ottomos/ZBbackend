SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*****************************************************************
KT – PnL Summary Report SIM
******************************************************************/
CREATE OR ALTER PROCEDURE apm.sel_PnLSummaryReport_KT
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

    DECLARE @Pairs TABLE (CurrencyPair NVARCHAR(10));
    INSERT INTO @Pairs VALUES
    ('AUD/USD'),('EUR/USD'),('GBP/USD'),('NZD/USD'),
    ('USD/AED'),('USD/BHD'),('USD/CAD'),('USD/CHF'),
    ('USD/DKK'),('USD/JPY'),('USD/KWD'),('USD/NOK'),
    ('USD/OMR'),('USD/QAR'),('USD/RUB'),('USD/SAR'),
    ('USD/SEK'),('USD/TRY'),
    ('XAG/USD'),('XAU/USD'),('XPD/USD'),('XPT/USD');

    ;WITH PnL AS
    (
        SELECT
            CurrencyPair,

            /* -20,000 ... +20,000 */
            (
                (ABS(CHECKSUM(NEWID())) % 4000000) / 100.0
                * CASE WHEN ABS(CHECKSUM(NEWID())) % 2 = 0 THEN -1 ELSE 1 END
            ) AS MatchingPNL,

            /* -40,000 ... +40,000 */
            (
                (ABS(CHECKSUM(NEWID())) % 8000000) / 100.0
                * CASE WHEN ABS(CHECKSUM(NEWID())) % 2 = 0 THEN -1 ELSE 1 END
            ) AS PositionPNL
        FROM @Pairs
    )
    SELECT
        CurrencyPair,

        CAST(
            @Prefix + RIGHT('000000' + CAST(ABS(CAST(MatchingPNL AS INT)) AS NVARCHAR),6)
            AS DECIMAL(18,2)
        ) * SIGN(MatchingPNL) AS MatchingPNL,

        CAST(
            @Prefix + RIGHT('000000' + CAST(ABS(CAST(PositionPNL AS INT)) AS NVARCHAR),6)
            AS DECIMAL(18,2)
        ) * SIGN(PositionPNL) AS PositionPNL,

        CAST(
            (
                CAST(
                    @Prefix + RIGHT('000000' + CAST(ABS(CAST(MatchingPNL AS INT)) AS NVARCHAR),6)
                    AS DECIMAL(18,2)
                ) * SIGN(MatchingPNL)
            )
            +
            (
                CAST(
                    @Prefix + RIGHT('000000' + CAST(ABS(CAST(PositionPNL AS INT)) AS NVARCHAR),6)
                    AS DECIMAL(18,2)
                ) * SIGN(PositionPNL)
            )
        AS DECIMAL(18,2)) AS TotalPNL
    FROM PnL
    ORDER BY CurrencyPair;
END
GO

/*****************************************************************
AUB – PnL Summary Report SIM
******************************************************************/
CREATE OR ALTER PROCEDURE apm.sel_PnLSummaryReport_AUB
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

    DECLARE @Pairs TABLE (CurrencyPair NVARCHAR(10));
    INSERT INTO @Pairs VALUES
    ('AUD/USD'),('EUR/USD'),('GBP/USD'),('NZD/USD'),
    ('USD/AED'),('USD/BHD'),('USD/CAD'),('USD/CHF'),
    ('USD/DKK'),('USD/JPY'),('USD/KWD'),('USD/NOK'),
    ('USD/OMR'),('USD/QAR'),('USD/RUB'),('USD/SAR'),
    ('USD/SEK'),('USD/TRY'),
    ('XAG/USD'),('XAU/USD'),('XPD/USD'),('XPT/USD');

    ;WITH PnL AS
    (
        SELECT
            CurrencyPair,
            (
                (ABS(CHECKSUM(NEWID())) % 4000000) / 100.0
                * CASE WHEN ABS(CHECKSUM(NEWID())) % 2 = 0 THEN -1 ELSE 1 END
            ) AS MatchingPNL,
            (
                (ABS(CHECKSUM(NEWID())) % 8000000) / 100.0
                * CASE WHEN ABS(CHECKSUM(NEWID())) % 2 = 0 THEN -1 ELSE 1 END
            ) AS PositionPNL
        FROM @Pairs
    )
    SELECT
        CurrencyPair,
        CAST(@Prefix + RIGHT('000000' + CAST(ABS(CAST(MatchingPNL AS INT)) AS NVARCHAR),6) AS DECIMAL(18,2)) * SIGN(MatchingPNL) AS MatchingPNL,
        CAST(@Prefix + RIGHT('000000' + CAST(ABS(CAST(PositionPNL AS INT)) AS NVARCHAR),6) AS DECIMAL(18,2)) * SIGN(PositionPNL) AS PositionPNL,
        CAST(
            (CAST(@Prefix + RIGHT('000000' + CAST(ABS(CAST(MatchingPNL AS INT)) AS NVARCHAR),6) AS DECIMAL(18,2)) * SIGN(MatchingPNL)) +
            (CAST(@Prefix + RIGHT('000000' + CAST(ABS(CAST(PositionPNL AS INT)) AS NVARCHAR),6) AS DECIMAL(18,2)) * SIGN(PositionPNL))
        AS DECIMAL(18,2)) AS TotalPNL
    FROM PnL
    ORDER BY CurrencyPair;
END
GO

/*****************************************************************
KFH – PnL Summary Report SIM
******************************************************************/
CREATE OR ALTER PROCEDURE apm.sel_PnLSummaryReport_KFH
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

    DECLARE @Pairs TABLE (CurrencyPair NVARCHAR(10));
    INSERT INTO @Pairs VALUES
    ('AUD/USD'),('EUR/USD'),('GBP/USD'),('NZD/USD'),
    ('USD/AED'),('USD/BHD'),('USD/CAD'),('USD/CHF'),
    ('USD/DKK'),('USD/JPY'),('USD/KWD'),('USD/NOK'),
    ('USD/OMR'),('USD/QAR'),('USD/RUB'),('USD/SAR'),
    ('USD/SEK'),('USD/TRY'),
    ('XAG/USD'),('XAU/USD'),('XPD/USD'),('XPT/USD');

    ;WITH PnL AS
    (
        SELECT
            CurrencyPair,
            (
                (ABS(CHECKSUM(NEWID())) % 4000000) / 100.0
                * CASE WHEN ABS(CHECKSUM(NEWID())) % 2 = 0 THEN -1 ELSE 1 END
            ) AS MatchingPNL,
            (
                (ABS(CHECKSUM(NEWID())) % 8000000) / 100.0
                * CASE WHEN ABS(CHECKSUM(NEWID())) % 2 = 0 THEN -1 ELSE 1 END
            ) AS PositionPNL
        FROM @Pairs
    )
    SELECT
        CurrencyPair,
        CAST(@Prefix + RIGHT('000000' + CAST(ABS(CAST(MatchingPNL AS INT)) AS NVARCHAR),6) AS DECIMAL(18,2)) * SIGN(MatchingPNL) AS MatchingPNL,
        CAST(@Prefix + RIGHT('000000' + CAST(ABS(CAST(PositionPNL AS INT)) AS NVARCHAR),6) AS DECIMAL(18,2)) * SIGN(PositionPNL) AS PositionPNL,
        CAST(
            (CAST(@Prefix + RIGHT('000000' + CAST(ABS(CAST(MatchingPNL AS INT)) AS NVARCHAR),6) AS DECIMAL(18,2)) * SIGN(MatchingPNL)) +
            (CAST(@Prefix + RIGHT('000000' + CAST(ABS(CAST(PositionPNL AS INT)) AS NVARCHAR),6) AS DECIMAL(18,2)) * SIGN(PositionPNL))
        AS DECIMAL(18,2)) AS TotalPNL
    FROM PnL
    ORDER BY CurrencyPair;
END
GO

--EXEC apm.sel_PnLSummaryReport_KFH

--   @Period = 'WTD',
--   @StartDate = '2026-02-12',
--   @EndDate   = '2026-02-18';

--EXEC apm.sel_CurrencySummaryReport_AUB

  -- @Period = 'MTD'


--EXEC apm.sel_PnLSummaryReport_KT

  -- @Period = 'TODAY'