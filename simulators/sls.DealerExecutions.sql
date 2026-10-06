/* ============================================================================
   sls sema olusturma
   Tablo olusturma : sls.DealerExecutions_KT / _AUB / _KFH
   SP olusturma    : sls.sel_DealerExecutions_KT / _AUB / _KFH
   ============================================================================ */

--ŞEMA--
USE StreambaseLogs;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = 'sls'
)
BEGIN
    EXEC ('CREATE SCHEMA sls');
END
GO

--TABLOLAR---

CREATE TABLE sls.DealerExecutions_AUB
(
    [DealerExecutionID] INT             IDENTITY(1,1) NOT NULL,
    [DealID]            NVARCHAR(30)    NULL,
    [Symbol]            NVARCHAR(10)    NULL,
    [Side]              NVARCHAR(10)    NULL,
    [BaseAmount]        DECIMAL(19,5)   NULL,
    [TranPrice]         DECIMAL(18,5)   NULL,
    [Price]             DECIMAL(18,5)   NULL,
    [CustomerID]        NVARCHAR(30)    NULL,
    [ValueDate]         DATE            NULL,
    [Type]              NVARCHAR(10)    NULL,
    [Producer]          NVARCHAR(10)    NULL,
    [SystemDate]        DATETIME        NULL,
    [User]              NVARCHAR(100)   NULL,

    CONSTRAINT PK_DealerExecutions_AUB PRIMARY KEY CLUSTERED ([DealerExecutionID])
);
GO

CREATE TABLE sls.DealerExecutions_KFH
(
    [DealerExecutionID] INT             IDENTITY(1,1) NOT NULL,
    [DealID]            NVARCHAR(30)    NULL,
    [Symbol]            NVARCHAR(10)    NULL,
    [Side]              NVARCHAR(10)    NULL,
    [BaseAmount]        DECIMAL(19,5)   NULL,
    [TranPrice]         DECIMAL(18,5)   NULL,
    [Price]             DECIMAL(18,5)   NULL,
    [CustomerID]        NVARCHAR(30)    NULL,
    [ValueDate]         DATE            NULL,
    [Type]              NVARCHAR(10)    NULL,
    [Producer]          NVARCHAR(10)    NULL,
    [SystemDate]        DATETIME        NULL,
    [User]              NVARCHAR(100)   NULL,

    CONSTRAINT PK_DealerExecutions_KFH PRIMARY KEY CLUSTERED ([DealerExecutionID])
);
GO

CREATE TABLE sls.DealerExecutions_KT
(
    [DealerExecutionID] INT             IDENTITY(1,1) NOT NULL,
    [DealID]            NVARCHAR(30)    NULL,
    [Symbol]            NVARCHAR(10)    NULL,
    [Side]              NVARCHAR(10)    NULL,
    [BaseAmount]        DECIMAL(19,5)   NULL,
    [TranPrice]         DECIMAL(18,5)   NULL,
    [Price]             DECIMAL(18,5)   NULL,
    [CustomerID]        NVARCHAR(30)    NULL,
    [ValueDate]         DATE            NULL,
    [Type]              NVARCHAR(10)    NULL,
    [Producer]          NVARCHAR(10)    NULL,
    [SystemDate]        DATETIME        NULL,
    [User]              NVARCHAR(100)   NULL,

    CONSTRAINT PK_DealerExecutions_KT PRIMARY KEY CLUSTERED ([DealerExecutionID])
);
GO

--SP'ler---

CREATE OR ALTER PROCEDURE sls.sel_DealerExecutions_KT
(
    @BeginDate  DATETIME = NULL,
    @EndDate    DATETIME = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    /* ------------------------------------------------------------------
       Parametre girilmezse:
         @BeginDate -> İçinde bulunulan haftanın başı (Pazartesi 00:00)
         @EndDate   -> Bugünün sonu (23:59:59.997)
       ------------------------------------------------------------------ */
    DECLARE @Today DATE = CAST(GETDATE() AS DATE);

    IF @BeginDate IS NULL
        SET @BeginDate = DATEADD(DAY, -((DATEDIFF(DAY, '19050101', @Today)) % 7), @Today);
        -- '19050101' bir Pazartesi'dir => DATEFIRST/LANGUAGE ayarından bağımsız çalışır.

    IF @EndDate IS NULL
        SET @EndDate = @Today;

    -- Saat bilgisi verilmemişse gün sonuna kadar al
    SET @BeginDate = CAST(CAST(@BeginDate AS DATE) AS DATETIME);
    SET @EndDate   = DATEADD(MILLISECOND, -3, DATEADD(DAY, 1, CAST(CAST(@EndDate AS DATE) AS DATETIME)));

    SELECT
        [DealerExecutionID],
        [DealID],
        [Symbol],
        [Side],
        [BaseAmount],
        [TranPrice],
        [Price],
        [CustomerID],
        [ValueDate],
        [Type],
        [Producer],
        [SystemDate],
        [User]
    FROM sls.DealerExecutions_KT WITH (NOLOCK)
    WHERE [SystemDate] >= @BeginDate
      AND [SystemDate] <= @EndDate
    ORDER BY [SystemDate] DESC, [DealerExecutionID] DESC;
END
GO

CREATE OR ALTER PROCEDURE sls.sel_DealerExecutions_AUB
(
    @BeginDate  DATETIME = NULL,
    @EndDate    DATETIME = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    /* ------------------------------------------------------------------
       Parametre girilmezse:
         @BeginDate -> İçinde bulunulan haftanın başı (Pazartesi 00:00)
         @EndDate   -> Bugünün sonu (23:59:59.997)
       ------------------------------------------------------------------ */
    DECLARE @Today DATE = CAST(GETDATE() AS DATE);

    IF @BeginDate IS NULL
        SET @BeginDate = DATEADD(DAY, -((DATEDIFF(DAY, '19050101', @Today)) % 7), @Today);
        -- '19050101' bir Pazartesi'dir => DATEFIRST/LANGUAGE ayarından bağımsız çalışır.

    IF @EndDate IS NULL
        SET @EndDate = @Today;

    -- Saat bilgisi verilmemişse gün sonuna kadar al
    SET @BeginDate = CAST(CAST(@BeginDate AS DATE) AS DATETIME);
    SET @EndDate   = DATEADD(MILLISECOND, -3, DATEADD(DAY, 1, CAST(CAST(@EndDate AS DATE) AS DATETIME)));

    SELECT
        [DealerExecutionID],
        [DealID],
        [Symbol],
        [Side],
        [BaseAmount],
        [TranPrice],
        [Price],
        [CustomerID],
        [ValueDate],
        [Type],
        [Producer],
        [SystemDate],
        [User]
    FROM sls.DealerExecutions_AUB WITH (NOLOCK)
    WHERE [SystemDate] >= @BeginDate
      AND [SystemDate] <= @EndDate
    ORDER BY [SystemDate] DESC, [DealerExecutionID] DESC;
END
GO

CREATE OR ALTER PROCEDURE sls.sel_DealerExecutions_KFH
(
    @BeginDate  DATETIME = NULL,
    @EndDate    DATETIME = NULL
)
AS
BEGIN
    SET NOCOUNT ON;

    /* ------------------------------------------------------------------
       Parametre girilmezse:
         @BeginDate -> İçinde bulunulan haftanın başı (Pazartesi 00:00)
         @EndDate   -> Bugünün sonu (23:59:59.997)
       ------------------------------------------------------------------ */
    DECLARE @Today DATE = CAST(GETDATE() AS DATE);

    IF @BeginDate IS NULL
        SET @BeginDate = DATEADD(DAY, -((DATEDIFF(DAY, '19050101', @Today)) % 7), @Today);
        -- '19050101' bir Pazartesi'dir => DATEFIRST/LANGUAGE ayarından bağımsız çalışır.

    IF @EndDate IS NULL
        SET @EndDate = @Today;

    -- Saat bilgisi verilmemişse gün sonuna kadar al
    SET @BeginDate = CAST(CAST(@BeginDate AS DATE) AS DATETIME);
    SET @EndDate   = DATEADD(MILLISECOND, -3, DATEADD(DAY, 1, CAST(CAST(@EndDate AS DATE) AS DATETIME)));

    SELECT
        [DealerExecutionID],
        [DealID],
        [Symbol],
        [Side],
        [BaseAmount],
        [TranPrice],
        [Price],
        [CustomerID],
        [ValueDate],
        [Type],
        [Producer],
        [SystemDate],
        [User]
    FROM sls.DealerExecutions_KFH WITH (NOLOCK)
    WHERE [SystemDate] >= @BeginDate
      AND [SystemDate] <= @EndDate
    ORDER BY [SystemDate] DESC, [DealerExecutionID] DESC;
END
GO
