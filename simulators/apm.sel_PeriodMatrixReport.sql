/****** apm.sel_PeriodMatrixReport_{KT,KFH,AUB} — Client Volume & PnL Trend (per-bucket random Symbol) ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE OR ALTER   PROCEDURE [apm].[sel_PeriodMatrixReport_KT]
    @Period       VARCHAR(10),          -- Today, WTD, MTD, QTD, YTD, CUSTOM
    @StartDate    DATE = NULL,          -- sadece CUSTOM için
    @EndDate      DATE = NULL           -- sadece CUSTOM için
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Today     DATE = CAST(GETDATE() AS DATE);
    DECLARE @RangeStart DATE;
    DECLARE @RangeEnd   DATE;

    IF @Period = 'Today'
    BEGIN
        SET @RangeStart = @Today;
        SET @RangeEnd   = @Today;
    END
    ELSE IF @Period = 'WTD'
    BEGIN
        SET @RangeStart = DATEADD(DAY, -((DATEDIFF(DAY, 0, @Today) + 2) % 7), @Today);
        SET @RangeEnd   = @Today;
    END
    ELSE IF @Period = 'MTD'
    BEGIN
        SET @RangeStart = DATEFROMPARTS(YEAR(@Today), MONTH(@Today), 1);
        SET @RangeEnd   = @Today;
    END
    ELSE IF @Period = 'QTD'
    BEGIN
        SET @RangeStart = DATEADD(QUARTER, DATEDIFF(QUARTER, 0, @Today), 0);
        SET @RangeEnd   = @Today;
    END
    ELSE IF @Period = 'YTD'
    BEGIN
        SET @RangeStart = DATEFROMPARTS(YEAR(@Today), 1, 1);
        SET @RangeEnd   = @Today;
    END
    ELSE IF @Period = 'CUSTOM'
    BEGIN
        IF @StartDate IS NULL OR @EndDate IS NULL
        BEGIN
            RAISERROR('CUSTOM icin @StartDate ve @EndDate zorunludur.', 16, 1);
            RETURN;
        END
        SET @RangeStart = @StartDate;
        SET @RangeEnd   = @EndDate;
    END
    ELSE
    BEGIN
        RAISERROR('Gecersiz @Period. (Today, WTD, MTD, QTD, YTD, CUSTOM)', 16, 1);
        RETURN;
    END

    IF @RangeStart > @RangeEnd
        RETURN;

    DECLARE @Pairs TABLE (id INT IDENTITY(1,1), pair VARCHAR(20));
    INSERT INTO @Pairs (pair) VALUES
        ('USD/BHD'), ('EUR/USD'), ('GBP/USD'), ('USD/KWD'),
        ('USD/SAR'), ('USD/AED'), ('USD/JPY'), ('EUR/BHD');

    DECLARE @Params TABLE (id INT IDENTITY(1,1), pname VARCHAR(50));
    INSERT INTO @Params (pname) VALUES
        ('ClosePositionAmount'), ('OpenPositionAmount'),
        ('MaxExposure'), ('SpreadLimit'), ('MarginRate');

    -- TODAY -> hourly buckets
    IF @Period = 'Today'
    BEGIN
        DECLARE @Now      DATETIME = GETDATE();
        DECLARE @DayStart DATETIME = CAST(@Today AS DATETIME);
        DECLARE @HourCount INT = DATEDIFF(HOUR, @DayStart, @Now) + 1;
        DECLARE @HourBucket INT;
        IF      @HourCount <= 10 SET @HourBucket = 1;
        ELSE IF @HourCount <= 20 SET @HourBucket = 2;
        ELSE                     SET @HourBucket = 3;

        ;WITH HourBuckets AS
        (
            SELECT @DayStart AS BucketStart, DATEADD(HOUR, @HourBucket, @DayStart) AS BucketEnd
            UNION ALL
            SELECT hb.BucketEnd, DATEADD(HOUR, @HourBucket, hb.BucketEnd)
            FROM HourBuckets hb
            WHERE hb.BucketEnd < @Now
        )
        SELECT
            hb.BucketStart AS StartDate,
            DATEADD(SECOND, -1, hb.BucketEnd) AS EndDate,
            CASE ABS(CHECKSUM(NEWID())) % 8
                 WHEN 0 THEN 'USD/BHD' WHEN 1 THEN 'EUR/USD' WHEN 2 THEN 'GBP/USD' WHEN 3 THEN 'USD/KWD'
                 WHEN 4 THEN 'USD/SAR' WHEN 5 THEN 'USD/AED' WHEN 6 THEN 'USD/JPY' ELSE 'EUR/BHD'
            END AS Symbol,
            CASE WHEN ABS(CHECKSUM(NEWID())) % 5 = 0
                 THEN '('
                    + (SELECT TOP 1 pair   FROM @Pairs  ORDER BY NEWID()) + ' '
                    + (SELECT TOP 1 pname  FROM @Params ORDER BY NEWID())
                    + ' old ' + CAST(CAST(ABS(CHECKSUM(NEWID())) % 100000 AS DECIMAL(18,2)) AS VARCHAR(30))
                    + ' new ' + CAST(CAST(ABS(CHECKSUM(NEWID())) % 100000 AS DECIMAL(18,2)) AS VARCHAR(30))
                    + ')'
                 ELSE NULL
            END AS ParamChange,
            CAST(ABS(CHECKSUM(NEWID())) % 1000000 + (ABS(CHECKSUM(NEWID())) % 100) / 100.0 AS DECIMAL(38,10)) AS ClientVolume,
            CAST((ABS(CHECKSUM(NEWID())) % 100000) - 50000 + (ABS(CHECKSUM(NEWID())) % 100) / 100.0 AS DECIMAL(38,10)) AS PnL
        FROM HourBuckets hb
        ORDER BY hb.BucketStart
        OPTION (MAXRECURSION 0);

        RETURN;
    END

    DECLARE @DayCount INT = DATEDIFF(DAY, @RangeStart, @RangeEnd) + 1;
    DECLARE @IntervalType VARCHAR(5);
    DECLARE @IntervalSize INT;
    IF      @DayCount <= 10  BEGIN SET @IntervalType='DAY';   SET @IntervalSize=1; END
    ELSE IF @DayCount <= 20  BEGIN SET @IntervalType='DAY';   SET @IntervalSize=2; END
    ELSE IF @DayCount <  28  BEGIN SET @IntervalType='DAY';   SET @IntervalSize=3; END
    ELSE IF @DayCount <  90  BEGIN SET @IntervalType='DAY';   SET @IntervalSize=7; END
    ELSE IF @DayCount <  270 BEGIN SET @IntervalType='MONTH'; SET @IntervalSize=1; END
    ELSE                     BEGIN SET @IntervalType='MONTH'; SET @IntervalSize=3; END

    ;WITH Buckets AS
    (
        SELECT
            CAST(@RangeStart AS DATE) AS BucketStart,
            CASE
                WHEN @IntervalType = 'DAY'
                    THEN CASE WHEN DATEADD(DAY, @IntervalSize - 1, @RangeStart) > @RangeEnd THEN @RangeEnd
                              ELSE DATEADD(DAY, @IntervalSize - 1, @RangeStart) END
                ELSE CASE WHEN DATEADD(DAY, -1, DATEADD(MONTH, @IntervalSize, @RangeStart)) > @RangeEnd THEN @RangeEnd
                          ELSE DATEADD(DAY, -1, DATEADD(MONTH, @IntervalSize, @RangeStart)) END
            END AS BucketEnd
        UNION ALL
        SELECT
            CAST(DATEADD(DAY, 1, b.BucketEnd) AS DATE) AS BucketStart,
            CASE
                WHEN @IntervalType = 'DAY'
                    THEN CASE WHEN DATEADD(DAY, @IntervalSize - 1, DATEADD(DAY, 1, b.BucketEnd)) > @RangeEnd THEN @RangeEnd
                              ELSE DATEADD(DAY, @IntervalSize - 1, DATEADD(DAY, 1, b.BucketEnd)) END
                ELSE CASE WHEN DATEADD(DAY, -1, DATEADD(MONTH, @IntervalSize, DATEADD(DAY, 1, b.BucketEnd))) > @RangeEnd THEN @RangeEnd
                          ELSE DATEADD(DAY, -1, DATEADD(MONTH, @IntervalSize, DATEADD(DAY, 1, b.BucketEnd))) END
            END AS BucketEnd
        FROM Buckets b
        WHERE b.BucketEnd < @RangeEnd
    )
    SELECT
        CAST(BucketStart AS DATETIME) AS StartDate,
        DATEADD(SECOND, -1, CAST(DATEADD(DAY, 1, BucketEnd) AS DATETIME)) AS EndDate,
        CASE ABS(CHECKSUM(NEWID())) % 8
             WHEN 0 THEN 'USD/BHD' WHEN 1 THEN 'EUR/USD' WHEN 2 THEN 'GBP/USD' WHEN 3 THEN 'USD/KWD'
             WHEN 4 THEN 'USD/SAR' WHEN 5 THEN 'USD/AED' WHEN 6 THEN 'USD/JPY' ELSE 'EUR/BHD'
        END AS Symbol,
        CASE WHEN ABS(CHECKSUM(NEWID())) % 4 = 0
             THEN '('
                + (SELECT TOP 1 pair   FROM @Pairs  ORDER BY NEWID()) + ' '
                + (SELECT TOP 1 pname  FROM @Params ORDER BY NEWID())
                + ' old ' + CAST(CAST(ABS(CHECKSUM(NEWID())) % 100000 AS DECIMAL(18,2)) AS VARCHAR(30))
                + ' new ' + CAST(CAST(ABS(CHECKSUM(NEWID())) % 100000 AS DECIMAL(18,2)) AS VARCHAR(30))
                + ')'
             ELSE NULL
        END AS ParamChange,
        CAST( (DATEDIFF(DAY, BucketStart, BucketEnd) + 1)
              * (ABS(CHECKSUM(NEWID())) % 1000000 + (ABS(CHECKSUM(NEWID())) % 100) / 100.0) AS DECIMAL(38,10)) AS ClientVolume,
        CAST( (DATEDIFF(DAY, BucketStart, BucketEnd) + 1)
              * ((ABS(CHECKSUM(NEWID())) % 100000) - 50000 + (ABS(CHECKSUM(NEWID())) % 100) / 100.0) AS DECIMAL(38,10)) AS PnL
    FROM Buckets
    ORDER BY BucketStart
    OPTION (MAXRECURSION 0);
END
GO

CREATE OR ALTER   PROCEDURE [apm].[sel_PeriodMatrixReport_KFH]
    @Period       VARCHAR(10),
    @StartDate    DATE = NULL,
    @EndDate      DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Today     DATE = CAST(GETDATE() AS DATE);
    DECLARE @RangeStart DATE;
    DECLARE @RangeEnd   DATE;

    IF @Period = 'Today'
    BEGIN
        SET @RangeStart = @Today; SET @RangeEnd = @Today;
    END
    ELSE IF @Period = 'WTD'
    BEGIN
        SET @RangeStart = DATEADD(DAY, -((DATEDIFF(DAY, 0, @Today) + 2) % 7), @Today); SET @RangeEnd = @Today;
    END
    ELSE IF @Period = 'MTD'
    BEGIN
        SET @RangeStart = DATEFROMPARTS(YEAR(@Today), MONTH(@Today), 1); SET @RangeEnd = @Today;
    END
    ELSE IF @Period = 'QTD'
    BEGIN
        SET @RangeStart = DATEADD(QUARTER, DATEDIFF(QUARTER, 0, @Today), 0); SET @RangeEnd = @Today;
    END
    ELSE IF @Period = 'YTD'
    BEGIN
        SET @RangeStart = DATEFROMPARTS(YEAR(@Today), 1, 1); SET @RangeEnd = @Today;
    END
    ELSE IF @Period = 'CUSTOM'
    BEGIN
        IF @StartDate IS NULL OR @EndDate IS NULL
        BEGIN
            RAISERROR('CUSTOM icin @StartDate ve @EndDate zorunludur.', 16, 1);
            RETURN;
        END
        SET @RangeStart = @StartDate; SET @RangeEnd = @EndDate;
    END
    ELSE
    BEGIN
        RAISERROR('Gecersiz @Period. (Today, WTD, MTD, QTD, YTD, CUSTOM)', 16, 1);
        RETURN;
    END

    IF @RangeStart > @RangeEnd
        RETURN;

    DECLARE @Pairs TABLE (id INT IDENTITY(1,1), pair VARCHAR(20));
    INSERT INTO @Pairs (pair) VALUES
        ('USD/BHD'), ('EUR/USD'), ('GBP/USD'), ('USD/KWD'),
        ('USD/SAR'), ('USD/AED'), ('USD/JPY'), ('EUR/BHD');

    DECLARE @Params TABLE (id INT IDENTITY(1,1), pname VARCHAR(50));
    INSERT INTO @Params (pname) VALUES
        ('ClosePositionAmount'), ('OpenPositionAmount'),
        ('MaxExposure'), ('SpreadLimit'), ('MarginRate');

    IF @Period = 'Today'
    BEGIN
        DECLARE @Now      DATETIME = GETDATE();
        DECLARE @DayStart DATETIME = CAST(@Today AS DATETIME);
        DECLARE @HourCount INT = DATEDIFF(HOUR, @DayStart, @Now) + 1;
        DECLARE @HourBucket INT;
        IF      @HourCount <= 10 SET @HourBucket = 1;
        ELSE IF @HourCount <= 20 SET @HourBucket = 2;
        ELSE                     SET @HourBucket = 3;

        ;WITH HourBuckets AS
        (
            SELECT @DayStart AS BucketStart, DATEADD(HOUR, @HourBucket, @DayStart) AS BucketEnd
            UNION ALL
            SELECT hb.BucketEnd, DATEADD(HOUR, @HourBucket, hb.BucketEnd)
            FROM HourBuckets hb
            WHERE hb.BucketEnd < @Now
        )
        SELECT
            hb.BucketStart AS StartDate,
            DATEADD(SECOND, -1, hb.BucketEnd) AS EndDate,
            CASE ABS(CHECKSUM(NEWID())) % 8
                 WHEN 0 THEN 'USD/BHD' WHEN 1 THEN 'EUR/USD' WHEN 2 THEN 'GBP/USD' WHEN 3 THEN 'USD/KWD'
                 WHEN 4 THEN 'USD/SAR' WHEN 5 THEN 'USD/AED' WHEN 6 THEN 'USD/JPY' ELSE 'EUR/BHD'
            END AS Symbol,
            CASE WHEN ABS(CHECKSUM(NEWID())) % 5 = 0
                 THEN '('
                    + (SELECT TOP 1 pair   FROM @Pairs  ORDER BY NEWID()) + ' '
                    + (SELECT TOP 1 pname  FROM @Params ORDER BY NEWID())
                    + ' old ' + CAST(CAST(ABS(CHECKSUM(NEWID())) % 100000 AS DECIMAL(18,2)) AS VARCHAR(30))
                    + ' new ' + CAST(CAST(ABS(CHECKSUM(NEWID())) % 100000 AS DECIMAL(18,2)) AS VARCHAR(30))
                    + ')'
                 ELSE NULL
            END AS ParamChange,
            CAST(ABS(CHECKSUM(NEWID())) % 1000000 + (ABS(CHECKSUM(NEWID())) % 100) / 100.0 AS DECIMAL(38,10)) AS ClientVolume,
            CAST((ABS(CHECKSUM(NEWID())) % 100000) - 50000 + (ABS(CHECKSUM(NEWID())) % 100) / 100.0 AS DECIMAL(38,10)) AS PnL
        FROM HourBuckets hb
        ORDER BY hb.BucketStart
        OPTION (MAXRECURSION 0);

        RETURN;
    END

    DECLARE @DayCount INT = DATEDIFF(DAY, @RangeStart, @RangeEnd) + 1;
    DECLARE @IntervalType VARCHAR(5);
    DECLARE @IntervalSize INT;
    IF      @DayCount <= 10  BEGIN SET @IntervalType='DAY';   SET @IntervalSize=1; END
    ELSE IF @DayCount <= 20  BEGIN SET @IntervalType='DAY';   SET @IntervalSize=2; END
    ELSE IF @DayCount <  28  BEGIN SET @IntervalType='DAY';   SET @IntervalSize=3; END
    ELSE IF @DayCount <  90  BEGIN SET @IntervalType='DAY';   SET @IntervalSize=7; END
    ELSE IF @DayCount <  270 BEGIN SET @IntervalType='MONTH'; SET @IntervalSize=1; END
    ELSE                     BEGIN SET @IntervalType='MONTH'; SET @IntervalSize=3; END

    ;WITH Buckets AS
    (
        SELECT
            CAST(@RangeStart AS DATE) AS BucketStart,
            CASE
                WHEN @IntervalType = 'DAY'
                    THEN CASE WHEN DATEADD(DAY, @IntervalSize - 1, @RangeStart) > @RangeEnd THEN @RangeEnd
                              ELSE DATEADD(DAY, @IntervalSize - 1, @RangeStart) END
                ELSE CASE WHEN DATEADD(DAY, -1, DATEADD(MONTH, @IntervalSize, @RangeStart)) > @RangeEnd THEN @RangeEnd
                          ELSE DATEADD(DAY, -1, DATEADD(MONTH, @IntervalSize, @RangeStart)) END
            END AS BucketEnd
        UNION ALL
        SELECT
            CAST(DATEADD(DAY, 1, b.BucketEnd) AS DATE) AS BucketStart,
            CASE
                WHEN @IntervalType = 'DAY'
                    THEN CASE WHEN DATEADD(DAY, @IntervalSize - 1, DATEADD(DAY, 1, b.BucketEnd)) > @RangeEnd THEN @RangeEnd
                              ELSE DATEADD(DAY, @IntervalSize - 1, DATEADD(DAY, 1, b.BucketEnd)) END
                ELSE CASE WHEN DATEADD(DAY, -1, DATEADD(MONTH, @IntervalSize, DATEADD(DAY, 1, b.BucketEnd))) > @RangeEnd THEN @RangeEnd
                          ELSE DATEADD(DAY, -1, DATEADD(MONTH, @IntervalSize, DATEADD(DAY, 1, b.BucketEnd))) END
            END AS BucketEnd
        FROM Buckets b
        WHERE b.BucketEnd < @RangeEnd
    )
    SELECT
        CAST(BucketStart AS DATETIME) AS StartDate,
        DATEADD(SECOND, -1, CAST(DATEADD(DAY, 1, BucketEnd) AS DATETIME)) AS EndDate,
        CASE ABS(CHECKSUM(NEWID())) % 8
             WHEN 0 THEN 'USD/BHD' WHEN 1 THEN 'EUR/USD' WHEN 2 THEN 'GBP/USD' WHEN 3 THEN 'USD/KWD'
             WHEN 4 THEN 'USD/SAR' WHEN 5 THEN 'USD/AED' WHEN 6 THEN 'USD/JPY' ELSE 'EUR/BHD'
        END AS Symbol,
        CASE WHEN ABS(CHECKSUM(NEWID())) % 4 = 0
             THEN '('
                + (SELECT TOP 1 pair   FROM @Pairs  ORDER BY NEWID()) + ' '
                + (SELECT TOP 1 pname  FROM @Params ORDER BY NEWID())
                + ' old ' + CAST(CAST(ABS(CHECKSUM(NEWID())) % 100000 AS DECIMAL(18,2)) AS VARCHAR(30))
                + ' new ' + CAST(CAST(ABS(CHECKSUM(NEWID())) % 100000 AS DECIMAL(18,2)) AS VARCHAR(30))
                + ')'
             ELSE NULL
        END AS ParamChange,
        CAST( (DATEDIFF(DAY, BucketStart, BucketEnd) + 1)
              * (ABS(CHECKSUM(NEWID())) % 1000000 + (ABS(CHECKSUM(NEWID())) % 100) / 100.0) AS DECIMAL(38,10)) AS ClientVolume,
        CAST( (DATEDIFF(DAY, BucketStart, BucketEnd) + 1)
              * ((ABS(CHECKSUM(NEWID())) % 100000) - 50000 + (ABS(CHECKSUM(NEWID())) % 100) / 100.0) AS DECIMAL(38,10)) AS PnL
    FROM Buckets
    ORDER BY BucketStart
    OPTION (MAXRECURSION 0);
END
GO

CREATE OR ALTER   PROCEDURE [apm].[sel_PeriodMatrixReport_AUB]
    @Period       VARCHAR(10),
    @StartDate    DATE = NULL,
    @EndDate      DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Today     DATE = CAST(GETDATE() AS DATE);
    DECLARE @RangeStart DATE;
    DECLARE @RangeEnd   DATE;

    IF @Period = 'Today'
    BEGIN
        SET @RangeStart = @Today; SET @RangeEnd = @Today;
    END
    ELSE IF @Period = 'WTD'
    BEGIN
        SET @RangeStart = DATEADD(DAY, -((DATEDIFF(DAY, 0, @Today) + 2) % 7), @Today); SET @RangeEnd = @Today;
    END
    ELSE IF @Period = 'MTD'
    BEGIN
        SET @RangeStart = DATEFROMPARTS(YEAR(@Today), MONTH(@Today), 1); SET @RangeEnd = @Today;
    END
    ELSE IF @Period = 'QTD'
    BEGIN
        SET @RangeStart = DATEADD(QUARTER, DATEDIFF(QUARTER, 0, @Today), 0); SET @RangeEnd = @Today;
    END
    ELSE IF @Period = 'YTD'
    BEGIN
        SET @RangeStart = DATEFROMPARTS(YEAR(@Today), 1, 1); SET @RangeEnd = @Today;
    END
    ELSE IF @Period = 'CUSTOM'
    BEGIN
        IF @StartDate IS NULL OR @EndDate IS NULL
        BEGIN
            RAISERROR('CUSTOM icin @StartDate ve @EndDate zorunludur.', 16, 1);
            RETURN;
        END
        SET @RangeStart = @StartDate; SET @RangeEnd = @EndDate;
    END
    ELSE
    BEGIN
        RAISERROR('Gecersiz @Period. (Today, WTD, MTD, QTD, YTD, CUSTOM)', 16, 1);
        RETURN;
    END

    IF @RangeStart > @RangeEnd
        RETURN;

    DECLARE @Pairs TABLE (id INT IDENTITY(1,1), pair VARCHAR(20));
    INSERT INTO @Pairs (pair) VALUES
        ('USD/BHD'), ('EUR/USD'), ('GBP/USD'), ('USD/KWD'),
        ('USD/SAR'), ('USD/AED'), ('USD/JPY'), ('EUR/BHD');

    DECLARE @Params TABLE (id INT IDENTITY(1,1), pname VARCHAR(50));
    INSERT INTO @Params (pname) VALUES
        ('ClosePositionAmount'), ('OpenPositionAmount'),
        ('MaxExposure'), ('SpreadLimit'), ('MarginRate');

    IF @Period = 'Today'
    BEGIN
        DECLARE @Now      DATETIME = GETDATE();
        DECLARE @DayStart DATETIME = CAST(@Today AS DATETIME);
        DECLARE @HourCount INT = DATEDIFF(HOUR, @DayStart, @Now) + 1;
        DECLARE @HourBucket INT;
        IF      @HourCount <= 10 SET @HourBucket = 1;
        ELSE IF @HourCount <= 20 SET @HourBucket = 2;
        ELSE                     SET @HourBucket = 3;

        ;WITH HourBuckets AS
        (
            SELECT @DayStart AS BucketStart, DATEADD(HOUR, @HourBucket, @DayStart) AS BucketEnd
            UNION ALL
            SELECT hb.BucketEnd, DATEADD(HOUR, @HourBucket, hb.BucketEnd)
            FROM HourBuckets hb
            WHERE hb.BucketEnd < @Now
        )
        SELECT
            hb.BucketStart AS StartDate,
            DATEADD(SECOND, -1, hb.BucketEnd) AS EndDate,
            CASE ABS(CHECKSUM(NEWID())) % 8
                 WHEN 0 THEN 'USD/BHD' WHEN 1 THEN 'EUR/USD' WHEN 2 THEN 'GBP/USD' WHEN 3 THEN 'USD/KWD'
                 WHEN 4 THEN 'USD/SAR' WHEN 5 THEN 'USD/AED' WHEN 6 THEN 'USD/JPY' ELSE 'EUR/BHD'
            END AS Symbol,
            CASE WHEN ABS(CHECKSUM(NEWID())) % 5 = 0
                 THEN '('
                    + (SELECT TOP 1 pair   FROM @Pairs  ORDER BY NEWID()) + ' '
                    + (SELECT TOP 1 pname  FROM @Params ORDER BY NEWID())
                    + ' old ' + CAST(CAST(ABS(CHECKSUM(NEWID())) % 100000 AS DECIMAL(18,2)) AS VARCHAR(30))
                    + ' new ' + CAST(CAST(ABS(CHECKSUM(NEWID())) % 100000 AS DECIMAL(18,2)) AS VARCHAR(30))
                    + ')'
                 ELSE NULL
            END AS ParamChange,
            CAST(ABS(CHECKSUM(NEWID())) % 1000000 + (ABS(CHECKSUM(NEWID())) % 100) / 100.0 AS DECIMAL(38,10)) AS ClientVolume,
            CAST((ABS(CHECKSUM(NEWID())) % 100000) - 50000 + (ABS(CHECKSUM(NEWID())) % 100) / 100.0 AS DECIMAL(38,10)) AS PnL
        FROM HourBuckets hb
        ORDER BY hb.BucketStart
        OPTION (MAXRECURSION 0);

        RETURN;
    END

    DECLARE @DayCount INT = DATEDIFF(DAY, @RangeStart, @RangeEnd) + 1;
    DECLARE @IntervalType VARCHAR(5);
    DECLARE @IntervalSize INT;
    IF      @DayCount <= 10  BEGIN SET @IntervalType='DAY';   SET @IntervalSize=1; END
    ELSE IF @DayCount <= 20  BEGIN SET @IntervalType='DAY';   SET @IntervalSize=2; END
    ELSE IF @DayCount <  28  BEGIN SET @IntervalType='DAY';   SET @IntervalSize=3; END
    ELSE IF @DayCount <  90  BEGIN SET @IntervalType='DAY';   SET @IntervalSize=7; END
    ELSE IF @DayCount <  270 BEGIN SET @IntervalType='MONTH'; SET @IntervalSize=1; END
    ELSE                     BEGIN SET @IntervalType='MONTH'; SET @IntervalSize=3; END

    ;WITH Buckets AS
    (
        SELECT
            CAST(@RangeStart AS DATE) AS BucketStart,
            CASE
                WHEN @IntervalType = 'DAY'
                    THEN CASE WHEN DATEADD(DAY, @IntervalSize - 1, @RangeStart) > @RangeEnd THEN @RangeEnd
                              ELSE DATEADD(DAY, @IntervalSize - 1, @RangeStart) END
                ELSE CASE WHEN DATEADD(DAY, -1, DATEADD(MONTH, @IntervalSize, @RangeStart)) > @RangeEnd THEN @RangeEnd
                          ELSE DATEADD(DAY, -1, DATEADD(MONTH, @IntervalSize, @RangeStart)) END
            END AS BucketEnd
        UNION ALL
        SELECT
            CAST(DATEADD(DAY, 1, b.BucketEnd) AS DATE) AS BucketStart,
            CASE
                WHEN @IntervalType = 'DAY'
                    THEN CASE WHEN DATEADD(DAY, @IntervalSize - 1, DATEADD(DAY, 1, b.BucketEnd)) > @RangeEnd THEN @RangeEnd
                              ELSE DATEADD(DAY, @IntervalSize - 1, DATEADD(DAY, 1, b.BucketEnd)) END
                ELSE CASE WHEN DATEADD(DAY, -1, DATEADD(MONTH, @IntervalSize, DATEADD(DAY, 1, b.BucketEnd))) > @RangeEnd THEN @RangeEnd
                          ELSE DATEADD(DAY, -1, DATEADD(MONTH, @IntervalSize, DATEADD(DAY, 1, b.BucketEnd))) END
            END AS BucketEnd
        FROM Buckets b
        WHERE b.BucketEnd < @RangeEnd
    )
    SELECT
        CAST(BucketStart AS DATETIME) AS StartDate,
        DATEADD(SECOND, -1, CAST(DATEADD(DAY, 1, BucketEnd) AS DATETIME)) AS EndDate,
        CASE ABS(CHECKSUM(NEWID())) % 8
             WHEN 0 THEN 'USD/BHD' WHEN 1 THEN 'EUR/USD' WHEN 2 THEN 'GBP/USD' WHEN 3 THEN 'USD/KWD'
             WHEN 4 THEN 'USD/SAR' WHEN 5 THEN 'USD/AED' WHEN 6 THEN 'USD/JPY' ELSE 'EUR/BHD'
        END AS Symbol,
        CASE WHEN ABS(CHECKSUM(NEWID())) % 4 = 0
             THEN '('
                + (SELECT TOP 1 pair   FROM @Pairs  ORDER BY NEWID()) + ' '
                + (SELECT TOP 1 pname  FROM @Params ORDER BY NEWID())
                + ' old ' + CAST(CAST(ABS(CHECKSUM(NEWID())) % 100000 AS DECIMAL(18,2)) AS VARCHAR(30))
                + ' new ' + CAST(CAST(ABS(CHECKSUM(NEWID())) % 100000 AS DECIMAL(18,2)) AS VARCHAR(30))
                + ')'
             ELSE NULL
        END AS ParamChange,
        CAST( (DATEDIFF(DAY, BucketStart, BucketEnd) + 1)
              * (ABS(CHECKSUM(NEWID())) % 1000000 + (ABS(CHECKSUM(NEWID())) % 100) / 100.0) AS DECIMAL(38,10)) AS ClientVolume,
        CAST( (DATEDIFF(DAY, BucketStart, BucketEnd) + 1)
              * ((ABS(CHECKSUM(NEWID())) % 100000) - 50000 + (ABS(CHECKSUM(NEWID())) % 100) / 100.0) AS DECIMAL(38,10)) AS PnL
    FROM Buckets
    ORDER BY BucketStart
    OPTION (MAXRECURSION 0);
END
GO
