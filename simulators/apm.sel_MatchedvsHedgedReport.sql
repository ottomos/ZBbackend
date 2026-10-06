SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
  KT
  Prefix:
    1st digit: random
    2nd digit: entity (KT=1)
    3rd digit: period (TODAY=1, WTD=2, MTD=3, QTD=4, YTD=5, CUSTOM=6)
    4th-5th digits (CUSTOM only): day values
*/
CREATE OR ALTER PROCEDURE [apm].[sel_MatchedvsHedgedReport_KT](
    @Period NVARCHAR(10),
    @StartDate DATE = NULL,
    @EndDate DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Entity NVARCHAR(1) = '1';
    DECLARE @PeriodCode NVARCHAR(1);
    DECLARE @DayPart NVARCHAR(10) = '';

    IF      @Period = 'TODAY' SET @PeriodCode = '1';
    ELSE IF @Period = 'WTD'   SET @PeriodCode = '2';
    ELSE IF @Period = 'MTD'   SET @PeriodCode = '3';
    ELSE IF @Period = 'QTD'   SET @PeriodCode = '4';
    ELSE IF @Period = 'YTD'   SET @PeriodCode = '5';
    ELSE IF @Period = 'CUSTOM'
    BEGIN
        SET @PeriodCode = '6';
        SET @DayPart = RIGHT('0' + CAST(DAY(@StartDate) AS NVARCHAR), 2)
                    + RIGHT('0' + CAST(DAY(@EndDate) AS NVARCHAR), 2);
    END
    ELSE
    BEGIN
        RAISERROR('Invalid Period', 16, 1);
        RETURN;
    END;

    DECLARE @FixedPart NVARCHAR(20) = @Entity + @PeriodCode + @DayPart;

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
            CAST((ABS(CHECKSUM(NEWID())) % 9) + 1 AS NVARCHAR(1)) AS RandDigit,
            (ABS(CHECKSUM(NEWID())) % 100000000) / 100.0 AS BaseAmount,
            (ABS(CHECKSUM(NEWID())) % 100) / 100.0 AS MatchedRatio,
            (ABS(CHECKSUM(NEWID())) % 100) / 100.0 AS InGroupRatio
        FROM @Symbols
    )
    SELECT
        Symbol,
        CAST(RandDigit + @FixedPart
            + RIGHT('000000' + CAST(CAST(BaseAmount * MatchedRatio AS INT) AS NVARCHAR), 6)
        AS DECIMAL(18,2)) AS MatchedAmountUSD,

        CAST(RandDigit + @FixedPart
            + RIGHT('000000' + CAST(CAST(BaseAmount * (1 - MatchedRatio) * InGroupRatio AS INT) AS NVARCHAR), 6)
        AS DECIMAL(18,2)) AS InGroupHedgeAmountUSD,

        CAST(RandDigit + @FixedPart
            + RIGHT('000000' + CAST(CAST(BaseAmount * (1 - MatchedRatio) * (1 - InGroupRatio) AS INT) AS NVARCHAR), 6)
        AS DECIMAL(18,2)) AS ExternalHedgeAmountUSD
    FROM BaseData
    ORDER BY Symbol;
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
  AUB
  Prefix:
    1st digit: random
    2nd digit: entity (AUB=2)
    3rd digit: period (TODAY=1, WTD=2, MTD=3, QTD=4, YTD=5, CUSTOM=6)
    4th-5th digits (CUSTOM only): day values
*/
CREATE OR ALTER PROCEDURE [apm].[sel_MatchedvsHedgedReport_AUB](
    @Period NVARCHAR(10),
    @StartDate DATE = NULL,
    @EndDate DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Entity NVARCHAR(1) = '2';
    DECLARE @PeriodCode NVARCHAR(1);
    DECLARE @DayPart NVARCHAR(10) = '';

    IF      @Period = 'TODAY' SET @PeriodCode = '1';
    ELSE IF @Period = 'WTD'   SET @PeriodCode = '2';
    ELSE IF @Period = 'MTD'   SET @PeriodCode = '3';
    ELSE IF @Period = 'QTD'   SET @PeriodCode = '4';
    ELSE IF @Period = 'YTD'   SET @PeriodCode = '5';
    ELSE IF @Period = 'CUSTOM'
    BEGIN
        SET @PeriodCode = '6';
        SET @DayPart = RIGHT('0' + CAST(DAY(@StartDate) AS NVARCHAR), 2)
                    + RIGHT('0' + CAST(DAY(@EndDate) AS NVARCHAR), 2);
    END
    ELSE
    BEGIN
        RAISERROR('Invalid Period', 16, 1);
        RETURN;
    END;

    DECLARE @FixedPart NVARCHAR(20) = @Entity + @PeriodCode + @DayPart;

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
            CAST((ABS(CHECKSUM(NEWID())) % 9) + 1 AS NVARCHAR(1)) AS RandDigit,
            (ABS(CHECKSUM(NEWID())) % 100000000) / 100.0 AS BaseAmount,
            (ABS(CHECKSUM(NEWID())) % 100) / 100.0 AS MatchedRatio,
            (ABS(CHECKSUM(NEWID())) % 100) / 100.0 AS InGroupRatio
        FROM @Symbols
    )
    SELECT
        Symbol,
        CAST(RandDigit + @FixedPart
            + RIGHT('000000' + CAST(CAST(BaseAmount * MatchedRatio AS INT) AS NVARCHAR), 6)
        AS DECIMAL(18,2)) AS MatchedAmountUSD,

        CAST(RandDigit + @FixedPart
            + RIGHT('000000' + CAST(CAST(BaseAmount * (1 - MatchedRatio) * InGroupRatio AS INT) AS NVARCHAR), 6)
        AS DECIMAL(18,2)) AS InGroupHedgeAmountUSD,

        CAST(RandDigit + @FixedPart
            + RIGHT('000000' + CAST(CAST(BaseAmount * (1 - MatchedRatio) * (1 - InGroupRatio) AS INT) AS NVARCHAR), 6)
        AS DECIMAL(18,2)) AS ExternalHedgeAmountUSD
    FROM BaseData
    ORDER BY Symbol;
END
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
  KFH
  Prefix:
    1st digit: random
    2nd digit: entity (KFH=3)
    3rd digit: period (TODAY=1, WTD=2, MTD=3, QTD=4, YTD=5, CUSTOM=6)
    4th-5th digits (CUSTOM only): day values
*/
CREATE OR ALTER PROCEDURE [apm].[sel_MatchedvsHedgedReport_KFH](
    @Period NVARCHAR(10),
    @StartDate DATE = NULL,
    @EndDate DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Entity NVARCHAR(1) = '3';
    DECLARE @PeriodCode NVARCHAR(1);
    DECLARE @DayPart NVARCHAR(10) = '';

    IF      @Period = 'TODAY' SET @PeriodCode = '1';
    ELSE IF @Period = 'WTD'   SET @PeriodCode = '2';
    ELSE IF @Period = 'MTD'   SET @PeriodCode = '3';
    ELSE IF @Period = 'QTD'   SET @PeriodCode = '4';
    ELSE IF @Period = 'YTD'   SET @PeriodCode = '5';
    ELSE IF @Period = 'CUSTOM'
    BEGIN
        SET @PeriodCode = '6';
        SET @DayPart = RIGHT('0' + CAST(DAY(@StartDate) AS NVARCHAR), 2)
                    + RIGHT('0' + CAST(DAY(@EndDate) AS NVARCHAR), 2);
    END
    ELSE
    BEGIN
        RAISERROR('Invalid Period', 16, 1);
        RETURN;
    END;

    DECLARE @FixedPart NVARCHAR(20) = @Entity + @PeriodCode + @DayPart;

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
            CAST((ABS(CHECKSUM(NEWID())) % 9) + 1 AS NVARCHAR(1)) AS RandDigit,
            (ABS(CHECKSUM(NEWID())) % 100000000) / 100.0 AS BaseAmount,
            (ABS(CHECKSUM(NEWID())) % 100) / 100.0 AS MatchedRatio,
            (ABS(CHECKSUM(NEWID())) % 100) / 100.0 AS InGroupRatio
        FROM @Symbols
    )
    SELECT
        Symbol,
        CAST(RandDigit + @FixedPart
            + RIGHT('000000' + CAST(CAST(BaseAmount * MatchedRatio AS INT) AS NVARCHAR), 6)
        AS DECIMAL(18,2)) AS MatchedAmountUSD,

        CAST(RandDigit + @FixedPart
            + RIGHT('000000' + CAST(CAST(BaseAmount * (1 - MatchedRatio) * InGroupRatio AS INT) AS NVARCHAR), 6)
        AS DECIMAL(18,2)) AS InGroupHedgeAmountUSD,

        CAST(RandDigit + @FixedPart
            + RIGHT('000000' + CAST(CAST(BaseAmount * (1 - MatchedRatio) * (1 - InGroupRatio) AS INT) AS NVARCHAR), 6)
        AS DECIMAL(18,2)) AS ExternalHedgeAmountUSD
    FROM BaseData
    ORDER BY Symbol;
END
GO
