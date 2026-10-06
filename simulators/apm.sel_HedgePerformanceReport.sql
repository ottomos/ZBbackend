SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*****************************************************************
 KT  — Symbol x HedgeType breakdown
******************************************************************/
CREATE OR ALTER PROCEDURE apm.sel_HedgePerformanceReport_KT
(
    @Period     NVARCHAR(10),
    @StartDate  DATE = NULL,
    @EndDate    DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET DATEFIRST 6;

    DECLARE @Today        DATE = CAST(GETDATE() AS DATE);
    DECLARE @WeekStart    DATE = DATEADD(DAY, -((DATEPART(WEEKDAY, @Today) + 6) % 7), @Today);
    DECLARE @MonthStart   DATE = DATEFROMPARTS(YEAR(@Today), MONTH(@Today), 1);
    DECLARE @QuarterStart DATE = DATEFROMPARTS(YEAR(@Today), ((DATEPART(QUARTER, @Today) - 1) * 3) + 1, 1);
    DECLARE @YearStart    DATE = DATEFROMPARTS(YEAR(@Today), 1, 1);
    DECLARE @PeriodStartDate DATE;
    DECLARE @PeriodEndDate   DATE = @Today;

    /* PERIOD HESAPLAMA */
    IF @Period = 'CUSTOM'
    BEGIN
        IF @StartDate IS NULL
        BEGIN
            RAISERROR('CUSTOM periyodu için @StartDate parametresi zorunludur!', 16, 1);
            RETURN;
        END

        SET @PeriodStartDate = @StartDate;
        SET @PeriodEndDate   = ISNULL(@EndDate, @Today);

        IF @PeriodStartDate > @PeriodEndDate
        BEGIN
            RAISERROR('Başlangıç tarihi bitiş tarihinden büyük olamaz!', 16, 1);
            RETURN;
        END

        IF @PeriodStartDate > @Today
        BEGIN
            RAISERROR('Başlangıç tarihi gelecekte olamaz!', 16, 1);
            RETURN;
        END
    END
    ELSE
    BEGIN
        IF @Period = 'TODAY'
            SET @PeriodStartDate = @Today;
        ELSE IF @Period = 'WTD'
            SET @PeriodStartDate = @WeekStart;
        ELSE IF @Period = 'MTD'
            SET @PeriodStartDate = @MonthStart;
        ELSE IF @Period = 'QTD'
            SET @PeriodStartDate = @QuarterStart;
        ELSE IF @Period = 'YTD'
            SET @PeriodStartDate = @YearStart;
        ELSE
        BEGIN
            RAISERROR('Geçersiz @Period değeri.', 16, 1);
            RETURN;
        END
    END

    /* RASTGELE VERİ HAVUZLARI */
    DECLARE @Symbols TABLE (Id INT IDENTITY(1,1), Symbol NVARCHAR(20));
    INSERT INTO @Symbols (Symbol)
    VALUES ('EUR/USD'), ('GBP/USD'), ('USD/TRY'), ('USD/JPY'),
           ('XAU/USD'), ('USD/CAD'), ('XAG/USD'), ('AUD/USD');

    DECLARE @Strategies TABLE (Id INT IDENTITY(1,1), HedgeType NVARCHAR(50));
    INSERT INTO @Strategies (HedgeType)
    VALUES ('TakeProfit'), ('StopLoss'), ('OverHedgeLimit'),
           ('EndOfDay'), ('ManuallyClosed'), ('Carry');

    /* Period aralığındaki gün sayısı (yoğunluk çarpanı için) */
    DECLARE @DayCount INT = DATEDIFF(DAY, @PeriodStartDate, @PeriodEndDate) + 1;
    IF @DayCount < 1 SET @DayCount = 1;

    ;WITH Combinations AS
    (
        SELECT
            s.Symbol,
            st.HedgeType,
            ABS(CHECKSUM(NEWID())) AS Rnd1,
            ABS(CHECKSUM(NEWID())) AS Rnd2,
            ABS(CHECKSUM(NEWID())) AS Rnd3
        FROM @Symbols s
        CROSS JOIN @Strategies st
        CROSS APPLY (SELECT ABS(CHECKSUM(NEWID())) % 10 AS FillRnd) AS f
        WHERE f.FillRnd < 8      -- ~%80 kombinasyon dolu (satır bazında değerlendirilir)
    )
    SELECT
        Symbol,
        HedgeType,
        CAST( (10000 + (Rnd1 % 490000)) * @DayCount * (0.5 + (Rnd2 % 100) / 100.0)
              AS DECIMAL(18,2) )                       AS ExecutionAmountUSD,
        ( (Rnd2 % 30) + 1 ) * @DayCount                AS ExecutionCount,
        CAST( ((Rnd3 % 200000) - 80000) * (@DayCount / 10.0 + 1)
              AS DECIMAL(18,2) )                       AS PositionPnL
    FROM Combinations
    ORDER BY ExecutionAmountUSD DESC;
END
GO

/*****************************************************************
 AUB  — Symbol x HedgeType breakdown
******************************************************************/
CREATE OR ALTER PROCEDURE apm.sel_HedgePerformanceReport_AUB
(
    @Period     NVARCHAR(10),
    @StartDate  DATE = NULL,
    @EndDate    DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET DATEFIRST 6;

    DECLARE @Today        DATE = CAST(GETDATE() AS DATE);
    DECLARE @WeekStart    DATE = DATEADD(DAY, -((DATEPART(WEEKDAY, @Today) + 6) % 7), @Today);
    DECLARE @MonthStart   DATE = DATEFROMPARTS(YEAR(@Today), MONTH(@Today), 1);
    DECLARE @QuarterStart DATE = DATEFROMPARTS(YEAR(@Today), ((DATEPART(QUARTER, @Today) - 1) * 3) + 1, 1);
    DECLARE @YearStart    DATE = DATEFROMPARTS(YEAR(@Today), 1, 1);
    DECLARE @PeriodStartDate DATE;
    DECLARE @PeriodEndDate   DATE = @Today;

    /* PERIOD HESAPLAMA */
    IF @Period = 'CUSTOM'
    BEGIN
        IF @StartDate IS NULL
        BEGIN
            RAISERROR('CUSTOM periyodu için @StartDate parametresi zorunludur!', 16, 1);
            RETURN;
        END

        SET @PeriodStartDate = @StartDate;
        SET @PeriodEndDate   = ISNULL(@EndDate, @Today);

        IF @PeriodStartDate > @PeriodEndDate
        BEGIN
            RAISERROR('Başlangıç tarihi bitiş tarihinden büyük olamaz!', 16, 1);
            RETURN;
        END

        IF @PeriodStartDate > @Today
        BEGIN
            RAISERROR('Başlangıç tarihi gelecekte olamaz!', 16, 1);
            RETURN;
        END
    END
    ELSE
    BEGIN
        IF @Period = 'TODAY'
            SET @PeriodStartDate = @Today;
        ELSE IF @Period = 'WTD'
            SET @PeriodStartDate = @WeekStart;
        ELSE IF @Period = 'MTD'
            SET @PeriodStartDate = @MonthStart;
        ELSE IF @Period = 'QTD'
            SET @PeriodStartDate = @QuarterStart;
        ELSE IF @Period = 'YTD'
            SET @PeriodStartDate = @YearStart;
        ELSE
        BEGIN
            RAISERROR('Geçersiz @Period değeri.', 16, 1);
            RETURN;
        END
    END

    /* RASTGELE VERİ HAVUZLARI */
    DECLARE @Symbols TABLE (Id INT IDENTITY(1,1), Symbol NVARCHAR(20));
    INSERT INTO @Symbols (Symbol)
    VALUES ('EUR/USD'), ('GBP/USD'), ('USD/TRY'), ('USD/JPY'),
           ('XAU/USD'), ('USD/CAD'), ('XAG/USD'), ('AUD/USD');

    DECLARE @Strategies TABLE (Id INT IDENTITY(1,1), HedgeType NVARCHAR(50));
    INSERT INTO @Strategies (HedgeType)
    VALUES ('TakeProfit'), ('StopLoss'), ('OverHedgeLimit'),
           ('EndOfDay'), ('ManuallyClosed'), ('Carry');

    /* Period aralığındaki gün sayısı (yoğunluk çarpanı için) */
    DECLARE @DayCount INT = DATEDIFF(DAY, @PeriodStartDate, @PeriodEndDate) + 1;
    IF @DayCount < 1 SET @DayCount = 1;

    ;WITH Combinations AS
    (
        SELECT
            s.Symbol,
            st.HedgeType,
            ABS(CHECKSUM(NEWID())) AS Rnd1,
            ABS(CHECKSUM(NEWID())) AS Rnd2,
            ABS(CHECKSUM(NEWID())) AS Rnd3
        FROM @Symbols s
        CROSS JOIN @Strategies st
        CROSS APPLY (SELECT ABS(CHECKSUM(NEWID())) % 10 AS FillRnd) AS f
        WHERE f.FillRnd < 8      -- ~%80 kombinasyon dolu (satır bazında değerlendirilir)
    )
    SELECT
        Symbol,
        HedgeType,
        CAST( (10000 + (Rnd1 % 490000)) * @DayCount * (0.5 + (Rnd2 % 100) / 100.0)
              AS DECIMAL(18,2) )                       AS ExecutionAmountUSD,
        ( (Rnd2 % 30) + 1 ) * @DayCount                AS ExecutionCount,
        CAST( ((Rnd3 % 200000) - 80000) * (@DayCount / 10.0 + 1)
              AS DECIMAL(18,2) )                       AS PositionPnL
    FROM Combinations
    ORDER BY ExecutionAmountUSD DESC;
END
GO

/*****************************************************************
 KFH  — Symbol x HedgeType breakdown
******************************************************************/
CREATE OR ALTER PROCEDURE apm.sel_HedgePerformanceReport_KFH
(
    @Period     NVARCHAR(10),
    @StartDate  DATE = NULL,
    @EndDate    DATE = NULL
)
AS
BEGIN
    SET NOCOUNT ON;
    SET DATEFIRST 6;

    DECLARE @Today        DATE = CAST(GETDATE() AS DATE);
    DECLARE @WeekStart    DATE = DATEADD(DAY, -((DATEPART(WEEKDAY, @Today) + 6) % 7), @Today);
    DECLARE @MonthStart   DATE = DATEFROMPARTS(YEAR(@Today), MONTH(@Today), 1);
    DECLARE @QuarterStart DATE = DATEFROMPARTS(YEAR(@Today), ((DATEPART(QUARTER, @Today) - 1) * 3) + 1, 1);
    DECLARE @YearStart    DATE = DATEFROMPARTS(YEAR(@Today), 1, 1);
    DECLARE @PeriodStartDate DATE;
    DECLARE @PeriodEndDate   DATE = @Today;

    /* PERIOD HESAPLAMA */
    IF @Period = 'CUSTOM'
    BEGIN
        IF @StartDate IS NULL
        BEGIN
            RAISERROR('CUSTOM periyodu için @StartDate parametresi zorunludur!', 16, 1);
            RETURN;
        END

        SET @PeriodStartDate = @StartDate;
        SET @PeriodEndDate   = ISNULL(@EndDate, @Today);

        IF @PeriodStartDate > @PeriodEndDate
        BEGIN
            RAISERROR('Başlangıç tarihi bitiş tarihinden büyük olamaz!', 16, 1);
            RETURN;
        END

        IF @PeriodStartDate > @Today
        BEGIN
            RAISERROR('Başlangıç tarihi gelecekte olamaz!', 16, 1);
            RETURN;
        END
    END
    ELSE
    BEGIN
        IF @Period = 'TODAY'
            SET @PeriodStartDate = @Today;
        ELSE IF @Period = 'WTD'
            SET @PeriodStartDate = @WeekStart;
        ELSE IF @Period = 'MTD'
            SET @PeriodStartDate = @MonthStart;
        ELSE IF @Period = 'QTD'
            SET @PeriodStartDate = @QuarterStart;
        ELSE IF @Period = 'YTD'
            SET @PeriodStartDate = @YearStart;
        ELSE
        BEGIN
            RAISERROR('Geçersiz @Period değeri.', 16, 1);
            RETURN;
        END
    END

    /* RASTGELE VERİ HAVUZLARI */
    DECLARE @Symbols TABLE (Id INT IDENTITY(1,1), Symbol NVARCHAR(20));
    INSERT INTO @Symbols (Symbol)
    VALUES ('EUR/USD'), ('GBP/USD'), ('USD/TRY'), ('USD/JPY'),
           ('XAU/USD'), ('USD/CAD'), ('XAG/USD'), ('AUD/USD');

    DECLARE @Strategies TABLE (Id INT IDENTITY(1,1), HedgeType NVARCHAR(50));
    INSERT INTO @Strategies (HedgeType)
    VALUES ('TakeProfit'), ('StopLoss'), ('OverHedgeLimit'),
           ('EndOfDay'), ('ManuallyClosed'), ('Carry');

    /* Period aralığındaki gün sayısı (yoğunluk çarpanı için) */
    DECLARE @DayCount INT = DATEDIFF(DAY, @PeriodStartDate, @PeriodEndDate) + 1;
    IF @DayCount < 1 SET @DayCount = 1;

    ;WITH Combinations AS
    (
        SELECT
            s.Symbol,
            st.HedgeType,
            ABS(CHECKSUM(NEWID())) AS Rnd1,
            ABS(CHECKSUM(NEWID())) AS Rnd2,
            ABS(CHECKSUM(NEWID())) AS Rnd3
        FROM @Symbols s
        CROSS JOIN @Strategies st
        CROSS APPLY (SELECT ABS(CHECKSUM(NEWID())) % 10 AS FillRnd) AS f
        WHERE f.FillRnd < 8      -- ~%80 kombinasyon dolu (satır bazında değerlendirilir)
    )
    SELECT
        Symbol,
        HedgeType,
        CAST( (10000 + (Rnd1 % 490000)) * @DayCount * (0.5 + (Rnd2 % 100) / 100.0)
              AS DECIMAL(18,2) )                       AS ExecutionAmountUSD,
        ( (Rnd2 % 30) + 1 ) * @DayCount                AS ExecutionCount,
        CAST( ((Rnd3 % 200000) - 80000) * (@DayCount / 10.0 + 1)
              AS DECIMAL(18,2) )                       AS PositionPnL
    FROM Combinations
    ORDER BY ExecutionAmountUSD DESC;
END
GO
