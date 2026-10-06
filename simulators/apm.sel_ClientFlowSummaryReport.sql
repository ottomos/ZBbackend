SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*****************************************************************
 KT – Client Flow Summary SIM
******************************************************************/
CREATE OR ALTER PROCEDURE apm.sel_ClientFlowSummaryReport_KT
(
    @Period NVARCHAR(10),
    @StartDate DATE = NULL,
    @EndDate   DATE = NULL
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

    DECLARE @Pairs TABLE (CurrencyPair NVARCHAR(15));
    INSERT INTO @Pairs VALUES
    ('AED/TRY'),('AUD/TRY'),('AUD/USD'),('CAD/TRY'),('CHF/TRY'),
    ('DKK/TRY'),('EUR/CAD'),('EUR/CHF'),('EUR/GBP'),('EUR/JPY'),
    ('EUR/RUB'),('EUR/SAR'),('EUR/TRY'),('EUR/USD'),('GBP/TRY'),
    ('GBP/USD'),('JPY/TRY'),('KWD/TRY'),('NOK/TRY'),('QAR/TRY'),
    ('RUB/TRY'),('SAR/TRY'),('SEK/TRY'),
    ('USD/AED'),('USD/CAD'),('USD/CHF'),('USD/JPY'),
    ('USD/SAR'),('USD/SEK'),('USD/TRY'),
    ('XAG/EUR'),('XAG/TRY'),('XAG/USD'),
    ('XAU/EUR'),('XAU/TRY'),('XAU/USD'),
    ('XPD/EUR'),('XPD/TRY'),('XPD/USD'),
    ('XPT/EUR'),('XPT/TRY'),('XPT/USD');

    ;WITH BaseData AS
    (
        SELECT
            CurrencyPair,
            (ABS(CHECKSUM(NEWID())) % 500000000) / 100.0 AS BaseAmount,
            (ABS(CHECKSUM(NEWID())) % 100) / 100.0       AS BuyRatio
        FROM @Pairs
    ),
    GeneratedData AS
    (
        SELECT
            CurrencyPair,

            CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount AS INT) AS NVARCHAR),6)
                AS DECIMAL(18,2)) AS TotalAmount,

            CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount * BuyRatio AS INT) AS NVARCHAR),6)
                AS DECIMAL(18,2)) AS ClientBuyAmount,

            CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount * (1-BuyRatio) AS INT) AS NVARCHAR),6)
                AS DECIMAL(18,2)) AS ClientSellAmount,

            CAST(
                CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount * BuyRatio AS INT) AS NVARCHAR),6) AS DECIMAL(18,2))
              - CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount * (1-BuyRatio) AS INT) AS NVARCHAR),6) AS DECIMAL(18,2))
            AS DECIMAL(18,2)) AS NetAmount,

            CAST(
                @Prefix
                + RIGHT(
                    '000000'
                    + CAST(
                        CAST(
                            ABS(
                                (
                                    CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount * BuyRatio AS INT) AS NVARCHAR),6) AS DECIMAL(18,2))
                                  - CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount * (1-BuyRatio) AS INT) AS NVARCHAR),6) AS DECIMAL(18,2))
                                )
                                * ((ABS(CHECKSUM(NEWID())) % 5 + 1) / 100.0)
                            ) AS INT
                        ) AS NVARCHAR
                    )
                ,6)
            AS DECIMAL(18,2))
            * CASE WHEN ABS(CHECKSUM(NEWID())) % 2 = 0 THEN 1 ELSE -1 END
            AS SalesPnL,

            CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount AS INT) AS NVARCHAR),6)
                AS DECIMAL(18,2))*1.2 AS TotalAmountUSD
        FROM BaseData
    )
    SELECT
        CurrencyPair,
        TotalAmount,
        ClientBuyAmount,
        ClientSellAmount,
        NetAmount,
        SalesPnL,
        TotalAmountUSD
    FROM
    (
        SELECT
            0 AS SortOrder,
            CurrencyPair,
            TotalAmount,
            ClientBuyAmount,
            ClientSellAmount,
            NetAmount,
            SalesPnL,
            TotalAmountUSD
        FROM GeneratedData

        UNION ALL

        SELECT
            1 AS SortOrder,
            'Total ($)' AS CurrencyPair,
            SUM(TotalAmount) AS TotalAmount,
            SUM(ClientBuyAmount) AS ClientBuyAmount,
            SUM(ClientSellAmount) AS ClientSellAmount,
            SUM(NetAmount) AS NetAmount,
            SUM(SalesPnL) AS SalesPnL,
            SUM(TotalAmountUSD) AS TotalAmountUSD
        FROM GeneratedData
    ) AS ResultData
    ORDER BY SortOrder, CurrencyPair;
END
GO

/*****************************************************************
 AUB – Client Flow Summary SIM
******************************************************************/
CREATE OR ALTER PROCEDURE apm.sel_ClientFlowSummaryReport_AUB
(
    @Period NVARCHAR(10),
    @StartDate DATE = NULL,
    @EndDate   DATE = NULL
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

    DECLARE @Pairs TABLE (CurrencyPair NVARCHAR(15));
    INSERT INTO @Pairs VALUES
    ('AED/TRY'),('AUD/TRY'),('AUD/USD'),('CAD/TRY'),('CHF/TRY'),
    ('DKK/TRY'),('EUR/CAD'),('EUR/CHF'),('EUR/GBP'),('EUR/JPY'),
    ('EUR/RUB'),('EUR/SAR'),('EUR/TRY'),('EUR/USD'),('GBP/TRY'),
    ('GBP/USD'),('JPY/TRY'),('KWD/TRY'),('NOK/TRY'),('QAR/TRY'),
    ('RUB/TRY'),('SAR/TRY'),('SEK/TRY'),
    ('USD/AED'),('USD/CAD'),('USD/CHF'),('USD/JPY'),
    ('USD/SAR'),('USD/SEK'),('USD/TRY'),
    ('XAG/EUR'),('XAG/TRY'),('XAG/USD'),
    ('XAU/EUR'),('XAU/TRY'),('XAU/USD'),
    ('XPD/EUR'),('XPD/TRY'),('XPD/USD'),
    ('XPT/EUR'),('XPT/TRY'),('XPT/USD');

    ;WITH BaseData AS
    (
        SELECT
            CurrencyPair,
            (ABS(CHECKSUM(NEWID())) % 500000000) / 100.0 AS BaseAmount,
            (ABS(CHECKSUM(NEWID())) % 100) / 100.0       AS BuyRatio
        FROM @Pairs
    ),
    GeneratedData AS
    (
        SELECT
            CurrencyPair,

            CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount AS INT) AS NVARCHAR),6)
                AS DECIMAL(18,2)) AS TotalAmount,

            CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount * BuyRatio AS INT) AS NVARCHAR),6)
                AS DECIMAL(18,2)) AS ClientBuyAmount,

            CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount * (1-BuyRatio) AS INT) AS NVARCHAR),6)
                AS DECIMAL(18,2)) AS ClientSellAmount,

            CAST(
                CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount * BuyRatio AS INT) AS NVARCHAR),6) AS DECIMAL(18,2))
              - CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount * (1-BuyRatio) AS INT) AS NVARCHAR),6) AS DECIMAL(18,2))
            AS DECIMAL(18,2)) AS NetAmount,

            CAST(
                @Prefix
                + RIGHT(
                    '000000'
                    + CAST(
                        CAST(
                            ABS(
                                (
                                    CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount * BuyRatio AS INT) AS NVARCHAR),6) AS DECIMAL(18,2))
                                  - CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount * (1-BuyRatio) AS INT) AS NVARCHAR),6) AS DECIMAL(18,2))
                                )
                                * ((ABS(CHECKSUM(NEWID())) % 5 + 1) / 100.0)
                            ) AS INT
                        ) AS NVARCHAR
                    )
                ,6)
            AS DECIMAL(18,2))
            * CASE WHEN ABS(CHECKSUM(NEWID())) % 2 = 0 THEN 1 ELSE -1 END
            AS SalesPnL,

            CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount AS INT) AS NVARCHAR),6)
                AS DECIMAL(18,2))*1.2 AS TotalAmountUSD
        FROM BaseData
    )
    SELECT
        CurrencyPair,
        TotalAmount,
        ClientBuyAmount,
        ClientSellAmount,
        NetAmount,
        SalesPnL,
        TotalAmountUSD
    FROM
    (
        SELECT
            0 AS SortOrder,
            CurrencyPair,
            TotalAmount,
            ClientBuyAmount,
            ClientSellAmount,
            NetAmount,
            SalesPnL,
            TotalAmountUSD
        FROM GeneratedData

        UNION ALL

        SELECT
            1 AS SortOrder,
            'Total ($)' AS CurrencyPair,
            SUM(TotalAmount) AS TotalAmount,
            SUM(ClientBuyAmount) AS ClientBuyAmount,
            SUM(ClientSellAmount) AS ClientSellAmount,
            SUM(NetAmount) AS NetAmount,
            SUM(SalesPnL) AS SalesPnL,
            SUM(TotalAmountUSD) AS TotalAmountUSD
        FROM GeneratedData
    ) AS ResultData
    ORDER BY SortOrder, CurrencyPair;
END
GO

/*****************************************************************
 KFH – Client Flow Summary SIM
******************************************************************/
CREATE OR ALTER PROCEDURE apm.sel_ClientFlowSummaryReport_KFH
(
    @Period NVARCHAR(10),
    @StartDate DATE = NULL,
    @EndDate   DATE = NULL
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

    DECLARE @Pairs TABLE (CurrencyPair NVARCHAR(15));
    INSERT INTO @Pairs VALUES
    ('AED/TRY'),('AUD/TRY'),('AUD/USD'),('CAD/TRY'),('CHF/TRY'),
    ('DKK/TRY'),('EUR/CAD'),('EUR/CHF'),('EUR/GBP'),('EUR/JPY'),
    ('EUR/RUB'),('EUR/SAR'),('EUR/TRY'),('EUR/USD'),('GBP/TRY'),
    ('GBP/USD'),('JPY/TRY'),('KWD/TRY'),('NOK/TRY'),('QAR/TRY'),
    ('RUB/TRY'),('SAR/TRY'),('SEK/TRY'),
    ('USD/AED'),('USD/CAD'),('USD/CHF'),('USD/JPY'),
    ('USD/SAR'),('USD/SEK'),('USD/TRY'),
    ('XAG/EUR'),('XAG/TRY'),('XAG/USD'),
    ('XAU/EUR'),('XAU/TRY'),('XAU/USD'),
    ('XPD/EUR'),('XPD/TRY'),('XPD/USD'),
    ('XPT/EUR'),('XPT/TRY'),('XPT/USD');

    ;WITH BaseData AS
    (
        SELECT
            CurrencyPair,
            (ABS(CHECKSUM(NEWID())) % 500000000) / 100.0 AS BaseAmount,
            (ABS(CHECKSUM(NEWID())) % 100) / 100.0       AS BuyRatio
        FROM @Pairs
    ),
    GeneratedData AS
    (
        SELECT
            CurrencyPair,

            CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount AS INT) AS NVARCHAR),6)
                AS DECIMAL(18,2)) AS TotalAmount,

            CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount * BuyRatio AS INT) AS NVARCHAR),6)
                AS DECIMAL(18,2)) AS ClientBuyAmount,

            CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount * (1-BuyRatio) AS INT) AS NVARCHAR),6)
                AS DECIMAL(18,2)) AS ClientSellAmount,

            CAST(
                CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount * BuyRatio AS INT) AS NVARCHAR),6) AS DECIMAL(18,2))
              - CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount * (1-BuyRatio) AS INT) AS NVARCHAR),6) AS DECIMAL(18,2))
            AS DECIMAL(18,2)) AS NetAmount,

            CAST(
                @Prefix
                + RIGHT(
                    '000000'
                    + CAST(
                        CAST(
                            ABS(
                                (
                                    CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount * BuyRatio AS INT) AS NVARCHAR),6) AS DECIMAL(18,2))
                                  - CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount * (1-BuyRatio) AS INT) AS NVARCHAR),6) AS DECIMAL(18,2))
                                )
                                * ((ABS(CHECKSUM(NEWID())) % 5 + 1) / 100.0)
                            ) AS INT
                        ) AS NVARCHAR
                    )
                ,6)
            AS DECIMAL(18,2))
            * CASE WHEN ABS(CHECKSUM(NEWID())) % 2 = 0 THEN 1 ELSE -1 END
            AS SalesPnL,

            CAST(@Prefix + RIGHT('000000' + CAST(CAST(BaseAmount AS INT) AS NVARCHAR),6)
                AS DECIMAL(18,2))*1.2 AS TotalAmountUSD
        FROM BaseData
    )
    SELECT
        CurrencyPair,
        TotalAmount,
        ClientBuyAmount,
        ClientSellAmount,
        NetAmount,
        SalesPnL,
        TotalAmountUSD
    FROM
    (
        SELECT
            0 AS SortOrder,
            CurrencyPair,
            TotalAmount,
            ClientBuyAmount,
            ClientSellAmount,
            NetAmount,
            SalesPnL,
            TotalAmountUSD
        FROM GeneratedData

        UNION ALL

        SELECT
            1 AS SortOrder,
            'Total ($)' AS CurrencyPair,
            SUM(TotalAmount) AS TotalAmount,
            SUM(ClientBuyAmount) AS ClientBuyAmount,
            SUM(ClientSellAmount) AS ClientSellAmount,
            SUM(NetAmount) AS NetAmount,
            SUM(SalesPnL) AS SalesPnL,
            SUM(TotalAmountUSD) AS TotalAmountUSD
        FROM GeneratedData
    ) AS ResultData
    ORDER BY SortOrder, CurrencyPair;
END
GO
