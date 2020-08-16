import pandas as pd
import numpy as np
from datetime import datetime
from datetime import timedelta
from datetime import date
import sqlalchemy as sq
from dateutil import rrule
from pandas.tseries.holiday import USFederalHolidayCalendar
from sqlalchemy.orm import sessionmaker
from config import config
from loggers import get_logger
from reporting.util import reporting_utility as utl
from models.reporting import Delinquent_Daily
from models.reporting import Delinquent_Historical


def active_loans(date, cnxn):

    """ function to load in data and transform it
        AmtOwedFwded is set to 0 for most recent loan
        AmtOwedAdj is calculated with new formula """

    query = (
        f"with x as (select * from reporting.vw_loans where LoanID not in"
        " ('1','34','57','312')) "
        ",x2 as (select LoanID, row_number() over "
        f"(partition by BusinessID order by Loandate desc) rnkactive from Loans where "
        f"Loandate <= '{date}'  and LoanID not in ('1','34','57','312'))"
        " select x.*, rnkactive from x left join x2 on x.LoanID = x2.LoanID"
    )

    df = pd.read_sql(query, cnxn)

    return df


def transform_active_loans(df):

    """ setting amount owed to be fowarded to 0 for most recent loan and
    calculating adjusted amount owed """

    df["LastTransaction"] = pd.to_datetime(df["LastTransaction"], errors="coerce")

    df["FirstTransaction"] = pd.to_datetime(df["FirstTransaction"], errors="coerce")

    df["AmtOwedFwded"] = np.where(df["rnkactive"] == 1, 0, df["AmtOwedFwded"])

    df["AmtOwedAdj"] = (
        df["AmtOwed"]
        + df["RefinancingFee"]
        + df["CarriedOverBal"]
        + df["RefinancedInterest"]
        - df["AmtOwedFwded"]
    )

    return df


def get_bank_data(date, cnxn):

    """ function to read in data from BankTransactions """

    query = (
        f"select LoanID, AmtPaid, AmtCharged "
        f"from reporting.vw_BankTransactions_transactions where Transdate <= '{date}';"
    )

    df = pd.read_sql(query, cnxn)

    return df


def get_paid_bank(df):

    """ replacing nulls """

    df["AmtPaid"] = np.where(df["AmtPaid"].isna(), 0, df["AmtPaid"])

    df["AmtCharged"] = np.where(df["AmtCharged"].isna(), 0, df["AmtCharged"])

    return df


def get_adj_data(date, cnxn):
    """ function to load in AdjustmentTransactions data
        refi fees for most recent loans are excluded """

    query = (
        f"select BusinessID, LoanID, OwedAmtAmtPaid, OwedAmtAmtCharged, TransID, "
        f" dense_rank() over (partition by BusinessID order by LoanID desc) rnk"
        f" from reporting.vw_AdjustmentTransactions_transactions where Transdate <= '{date}' "
        "and TransID != 15 and TransID != 4;"
    )

    df = pd.read_sql(query, cnxn)
    return df


def get_paid_adj(df):

    """ replacing nulls """

    df["OwedAmtAmtPaid"] = np.where(df["OwedAmtAmtPaid"].isna(), 0, df["OwedAmtAmtPaid"])

    df["OwedAmtAmtCharged"] = np.where(df["OwedAmtAmtCharged"].isna(), 0, df["OwedAmtAmtCharged"])

    return df


def aggregate(df, credit, debit, varname):

    """ getting total amount paid minus fees for balance calculations """

    agg = df.groupby("LoanID")[[credit, debit]].sum()

    agg = pd.DataFrame(agg)

    agg[varname] = agg[credit] - agg[debit]

    agg.drop([credit, debit], axis=1, inplace=True)

    return agg


def get_BankTransactions_date_data(date, cnxn):
    """ finds minimum BankTransactions transaction date by LoanID """

    query = (
        f"select LoanID, Transdate "
        f"from reporting.vw_BankTransactions_transactions where Transdate <= '{date}';"
    )

    df = pd.read_sql(query, cnxn)

    return df


def min_BankTransactions_date(df):

    mindate = df.groupby("LoanID")["Transdate"].min()

    mindate = pd.DataFrame(mindate)

    mindate.rename(columns={"Transdate": "firstBankTransactionsdate"}, inplace=True)

    return mindate


def get_business_days(df, date):

    """ function to get # business days between the start date of a loan and the date of analysis for daily loans
        excludes federal holidays """

    usa = USFederalHolidayCalendar()

    holiday = usa.holidays(start="2009-01-01", end=date)

    holiday = holiday.astype(str)

    enddate = date + timedelta(days=1)

    if df["PaymentSchedule"] == "weekly":
        return np.nan
    else:
        return np.busday_count(
            df["bdstartdate"].date(), enddate.date(), holidays=holiday
        )


def weeks_between(start_date, end_date):

    """ function to get the # weeks between the start date of a loan and the date of analysis for weekly loans """

    weeks = rrule.rrule(rrule.WEEKLY, dtstart=start_date, until=end_date)

    return weeks.count()


def get_weeks(df, date):

    """ function to return the # weeks for weekly loans """

    if df["PaymentSchedule"] == "weekly":
        return weeks_between(df["bdstartdate"], date)
    else:
        return np.nan


def date_by_subtracting_business_minutes(subtract_minutes, from_date):

    """ getting the number of business days past since start of loan """

    usa = USFederalHolidayCalendar()

    holiday = usa.holidays(start="2009-01-01", end=from_date)

    holiday = holiday.astype(str)

    business_minutes_to_subtract = subtract_minutes

    current_date = from_date

    while business_minutes_to_subtract > 0:
        current_date -= timedelta(minutes=1)
        weekday = current_date.weekday()
        if weekday >= 5:  # sunday = 6
            continue
        if current_date in holiday:
            continue
        business_minutes_to_subtract -= 1
    return current_date


def expectedamt(df):

    """ function to return expected amt paid as of analysis date for loans """

    if df["DailyPayment"] == 0:
        return np.nan
    elif df["PaymentSchedule"] == "weekly":
        return df["DailyPayment"] * 5 * df["weekspassed"]
    else:
        return df["DailyPayment"] * df["dayspassed"]


def get_today():

    """ getting date of analysis (current), today minus 2 business days """

    today = date.today()

    bd = pd.tseries.offsets.BusinessDay(n=-2)

    minus2bd = today + bd

    return minus2bd


def get_trn_agg(bank, adj):

    """ getting and aggregating transaction data to get total amount
    paid to date """

    aggbank = aggregate(bank, "AmtPaid", "AmtCharged", "bankamt")

    aggadj = aggregate(adj, "OwedAmtAmtPaid", "OwedAmtAmtCharged", "adjamt")

    agg = pd.merge(aggbank, aggadj, how="outer", on="LoanID")

    agg["bankamt"] = np.where(agg["bankamt"].isna(), 0, agg["bankamt"])

    agg["adjamt"] = np.where(agg["adjamt"].isna(), 0, agg["adjamt"])

    agg["AmtPaidTodate"] = agg["bankamt"] + agg["adjamt"]

    agg.drop(["bankamt", "adjamt"], axis=1, inplace=True)

    return agg


def merge_data(df1, df2, df3):

    """ merging loan and transaction data """

    df = pd.merge(df1, df2, how="left", on="LoanID")

    df = pd.merge(df, df3, how="left", on="LoanID")

    return df


def pymnt_start(df):

    """ getting appropriate payment start date """

    df["firstBankTransactionsdate"] = pd.to_datetime(df["firstBankTransactionsdate"])

    return np.where(
        df["firstBankTransactionsdate"].isna(), df["FirstTransaction"], df["firstBankTransactionsdate"]
    )


def amt_paid(df):

    """ consolidating amt paid for edge cases """

    return np.where(df["AmtPaidTodate"].isna(), 0, df["AmtPaidTodate"])


def select_df(df, today):

    """ filtering df for edge cases and active loans """

    df = df[df["Loandate"] <= today]

    df = df[df["DailyPayment"] > 1]

    return df[df["loanBalance"] > 1]


def time_passed(df, today):

    """ getting days and weeks passed for each loan,
    0 for loans not yet started payment or invalid start date """

    df["dayspassed"] = df[["PaymentSchedule", "bdstartdate"]].apply(
        get_business_days, args=[today], axis=1
    )

    df["dayspassed"] = np.where(
        (df["bdstartdate"] > today)
        | (df["bdstartdate"] < pd.to_datetime("2011-01-01"))
        | (df["firstBankTransactionsdate"].isna()),
        0,
        df["dayspassed"],
    )

    df["weekspassed"] = df[["PaymentSchedule", "bdstartdate"]].apply(
        get_weeks, args=[today], axis=1
    )

    df["weekspassed"] = np.where(
        (df["bdstartdate"] > today)
        | (df["bdstartdate"] < pd.to_datetime("2011-01-01"))
        | (df["firstBankTransactionsdate"].isna()),
        0,
        df["weekspassed"],
    )

    return df


def delinquent_amount(df):

    """ calculating amt delinquent """

    df["AmtDelinquent"] = np.where(
        df["ExpectedAmt"] <= df["AmtOwedAdj"],
        df["ExpectedAmt"] - df["AmtPaidTodatehack"],
        df["AmtOwedAdj"] - df["AmtPaidTodatehack"],
    )

    return np.where(df["AmtDelinquent"] < 0, 0, df["AmtDelinquent"])


def get_caldays(df, today):

    """ getting calendar days delinquent """

    df["minutesbehind"] = df["pymnts_delinquent"] * 24 * 60

    df["minutesbehind"] = round(df["minutesbehind"])

    df["minutesbehind"] = np.where(
        df["minutesbehind"] == np.inf, 1, df["minutesbehind"]
    )

    df["minutesbehind"] = df.minutesbehind.apply(lambda x: int(x))

    df["datebehind"] = df.minutesbehind.apply(
        lambda x: date_by_subtracting_business_minutes(x, today)
    )

    df["CalDays"] = today - df["datebehind"]

    return df["CalDays"] / timedelta(days=1)


def loan_term_calc(df):

    """ getting the remaining loan_term when correcting for overdue loans """

    if (df["calcloan_term"] - df["loan_term_diff"]) < 0:
        return df["neg_months_convert"]
    else:
        return df["calcloan_term"] - df["loan_term_diff"]


def loan_term_diff(x, today):
    return (today - x) / np.timedelta64(1, "M")


def get_remain_loan_term(df, today):

    """ calculating the difference in passed loan_term versus full loan_term OR payments left in calendar days
      to use  when calculating remaining loan_term """

    df["loan_term_diff"] = df.Loandate.apply(loan_term_diff, args=(today,))

    df["amt_left_to_pay"] = df["AmtOwedAdj"] - df["AmtPaidTodatehack"]

    df["paymnts_left"] = df["amt_left_to_pay"] / df["DailyPayment"]

    df["paymnts_left"] = np.where(
        df["paymnts_left"] < 5,
        df["paymnts_left"],
        np.floor(df["paymnts_left"] / 5) * 7 + (df["paymnts_left"] % 5),
    )

    df["neg_months_convert"] = df["paymnts_left"] / 30

    df["remaining_term"] = df[["calcloan_term", "loan_term_diff", "neg_months_convert"]].apply(
        loan_term_calc, axis=1
    )

    return df


def days_in_last_week(df, today):

    """ getting the amount of days passed in the last week """

    lastmonday = today - timedelta(days=today.weekday())

    df["daysinlastweek"] = today - lastmonday

    df["daysinlastweek"] = df["daysinlastweek"] + timedelta(days=1)

    df["daysinlastweek"] = df["daysinlastweek"].days

    df["daysinlastweek"] = np.float64(df["daysinlastweek"])

    return df


def clean_caldays(df, today):

    """ returing # payments delinquent for calendar days behind when
    the payments are less than the days in this week so far """

    df = df.apply(days_in_last_week, args=[today], axis=1)

    df["CalDays"] = np.where(
        df["pymnts_delinquent"] <= df["daysinlastweek"],
        df["pymnts_delinquent"],
        df["CalDays"],
    )

    df["CalDays"] = np.where(df["pymnts_delinquent"] < 0.001, 0, df["CalDays"])

    return np.where(df["minutesbehind"] == 1, np.nan, df["CalDays"])


def pyment_info(df):

    """ getting the amount of payments expected to date and payments
    actually paid to date """

    df["pymnts_expected"] = df["dayspassed"].fillna(df["weekspassed"])

    df["pymnts_paid"] = np.where(
        df["PaymentSchedule"] == "weekly",
        df["AmtPaidTodatehack"] / (df["DailyPayment"] * 5),
        df["AmtPaidTodatehack"] / df["DailyPayment"],
    )

    return df


def bins(df):

    """ binning vars of analysis """

    df["DelinquencyBins"] = pd.cut(
        df["CalDays"],
        [-1, 4, 15, 30, 60, 90, 120, 150, 180, np.inf],
        labels=[
            "0-3",
            "4-14",
            "15-29",
            "30-59",
            "60-89",
            "90-119",
            "120-149",
            "150-179",
            "180+",
        ],
        right=False,
    )

    df["DelinquencyBins"] = np.where(
        df["CalDays"] == np.inf, np.nan, df["DelinquencyBins"]
    )

    df["AnnualRevenueBins"] = np.where(
        df.AnnualRevenue.isnull(),
        "No Rev Info",
        pd.cut(
            df["AnnualRevenue"],
            [
                -np.inf,
                120000,
                175000,
                200000,
                500000,
                1000000,
                2000000,
                5000000,
                10000000,
                np.inf,
            ],
            labels=[
                "0-119K",
                "120-174K",
                "175-199K",
                "200-499K",
                "500-999K",
                "1-2M",
                "2-5M",
                "5-10M",
                ">=10M",
            ],
            right=False,
        ),
    )

    df["AnnualRevenueBins2"] = np.where(
        df.AnnualRevenue.isnull(),
        "No Rev Info",
        pd.cut(
            df["AnnualRevenue"],
            [-np.inf, 175000, 200000, np.inf],
            labels=["0-174K", "175-199K", "200K+"],
            right=False,
        ),
    )

    df["YIBbins"] = np.where(
        df.YearsInBusiness.isnull(),
        "No Info",
        pd.cut(
            df["YearsInBusiness"],
            [
                -np.inf,
                0,
                1,
                2,
                3,
                4,
                5,
                6,
                7,
                8,
                9,
                14,
                19,
                24,
                29,
                34,
                39,
                44,
                49,
                99,
                np.inf,
            ],
            labels=[
                "0",
                "1",
                "2",
                "3",
                "4",
                "5",
                "6",
                "7",
                "8",
                "9",
                "10-14",
                "15-19",
                "20-24",
                "25-29",
                "30-34",
                "35-39",
                "40-44",
                "45-49",
                "50-99",
                "100+",
            ],
        ),
    )

    df["CreditScorebins"] = pd.cut(
        df["CreditScore"],
        [400, 450, 500, 550, 600, 650, 700, 750, 800, 850, 900],
        labels=[
            "400-449",
            "450-499",
            "500-549",
            "550-599",
            "600-649",
            "650-699",
            "700-749",
            "750-799",
            "800-849",
            "850-899",
        ],
        right=False,
    )

    df["CreditScore2bins"] = pd.cut(
        df["CreditScore2"],
        [400, 450, 500, 550, 600, 650, 700, 750, 800, 850, 900],
        labels=[
            "400-449",
            "450-499",
            "500-549",
            "550-599",
            "600-649",
            "650-699",
            "700-749",
            "750-799",
            "800-849",
            "850-899",
        ],
        right=False,
    )

    return df


def data_cleaning(df):

    df["caldays"] = np.where(df["caldays"] == np.inf, np.nan, df["caldays"])

    df["delinquent_payments"] = np.where(
        df["delinquent_payments"] == np.inf, np.nan, df["delinquent_payments"]
    )

    df["expected_payments"] = np.where(
        df["expected_payments"] == np.inf, np.nan, df["expected_payments"]
    )

    df["paid_payments"] = np.where(
        df["paid_payments"] == np.inf, np.nan, df["paid_payments"]
    )

    return df


def modified_pymnt_56(df):

    """ one hard coded edge case """

    if df["dayspassed"] <= 103:
        return df["dayspassed"] * 200
    elif df["dayspassed"] > 103:
        return (103 * 200) + ((df["dayspassed"] - 103) * 500)


def expectedamt_cleaning(df, today):

    """ more hard coded edge cases
    and making sure expected amount doesn't exceed amount owed """

    nweeks = weeks_between(pd.to_datetime("2020-7-15"), today)

    df["ExpectedAmt"] = np.where(
        df["LoanID"] == 27, 50600 + (4500 * nweeks), df["ExpectedAmt"]
    )

    df["ExpectedAmt"] = np.where(
        df["LoanID"] == 99,
        (55 * 213) + ((df["dayspassed"] - 55) * 700),
        df["ExpectedAmt"],
    )

    df["ExpectedAmt"] = np.where(
        df["LoanID"] == 56,
        df.apply(modified_pymnt_56, axis=1),
        df["ExpectedAmt"],
    )

    df["ExpectedAmt"] = np.where(
        df["ExpectedAmt"] > df["AmtOwedAdj"], df["AmtOwedAdj"], df["ExpectedAmt"]
    )

    return df


def remove_and_rename(df):

    """ removing extraneous columns """

    df = df[
        [
            "LoanID",
            "BusinessID",
            "Loandate",
            "FirstTransaction",
            "firstBankTransactionsdate",
            "loanAmt",
            "calcloan_term",
            "AmtOwed",
            "CarriedOverBal",
            "RefinancingFee",
            "AmtOwedFwded",
            "AmtOwedAdj",
            "AmtPaidTodatehack",
            "loanBalance",
            "DailyPayment",
            "dayspassed",
            "weekspassed",
            "ExpectedAmt",
            "AmtDelinquent",
            "CalDays",
            "pymnts_delinquent",
            "PaymentSchedule",
            "DelinquencyBins",
            "AnnualRevenueBins",
            "AnnualRevenueBins2",
            "YIBbins",
            "CreditScorebins",
            "CreditScore2bins",
            "CreditScore",
            "CreditScore2",
            "YearsInBusiness",
            "AnnualRevenue",
            "AppType",
            "industry1",
            "industry2",
            "industry3",
            "industry4",
            "RepType",
            "date",
            "remaining_term",
            "pymnts_expected",
            "pymnts_paid",
        ]
    ]

    col_names = [
        "loan_id",
        "first_BusinessID",
        "loan_date",
        "first_trans",
        "first_BankTransactions_date",
        "loan_amt",
        "loan_term",
        "loan_OwedAmt",
        "fwded_bal",
        "fee",
        "OwedAmt_balance_tofwd",
        "AmtOwedAdj",
        "paid_amt_to_date",
        "loan_balance",
        "daily_payment",
        "days",
        "weeks",
        "expected_amount",
        "amt_deliquent",
        "calandar_days",
        "delinquent_payments",
        "payment_type",
        "bins_delinquency",
        "revenue_bins",
        "revenue_bins2",
        "years_in_business_bins",
        "CreditScore_bins",
        "CreditScore2_bins",
        "CreditScore",
        "CreditScore2",
        "years_in_business",
        "revenue",
        "application_type",
        "industry1",
        "industry2",
        "industry3",
        "industry4",
        "representative_type",
        "date",
        "remaining_term",
        "expected_payments",
        "paid_payments",
    ]

    df.columns = col_names

    return df


def loan_balance(df):

    """ getting balanc """

    return df["AmtOwedAdj"] - df["AmtPaidTodatehack"]


def get_pymnts_delinquent(df):

    return df["AmtDelinquent"] / df["DailyPayment"]


def get_date():

    """ for historical data uploads, replacing analysis date with
    first of the month date """

    date = datetime.today().replace(day=1)

    date = date.strftime("%Y-%m-%d")

    return pd.to_datetime(date)


def date_var(upload_type):

    if upload_type == "daily":
        return get_today()
    elif upload_type == "historical":
        return get_date()


def get_df(db_connection, db2_session, upload_type):

    log.info("beginning process")

    """ setting date of analysis to today """

    today = get_today()

    """ loading in data """

    activefunds = active_loans(today, db_connection)

    activefunds = transform_active_loans(activefunds)

    BankTransactionsmindate = get_BankTransactions_date_data(today, db_connection)

    BankTransactionsmindate = min_BankTransactions_date(BankTransactionsmindate)

    """ aggregating BankTransactions and AdjustmentTransactions data and combining them to get amt paid to date """

    bank = get_bank_data(today, db_connection)

    bank = get_paid_bank(bank)

    adj = get_adj_data(today, db_connection)

    adj = get_paid_adj(adj)

    agg = get_trn_agg(bank, adj)

    log.info(
        "data aggregation complete, " f"{agg.shape[0]} rows, {agg.shape[1]} columns"
    )

    log.info(
        "data loading complete, "
        f"{activefunds.shape[0]} rows, {activefunds.shape[1]} columns"
    )

    """ merging datasets to main df """

    df = merge_data(activefunds, agg, BankTransactionsmindate)

    log.info("dataset merge complete, " f"{df.shape[0]} rows, {df.shape[1]} columns")

    """ getting payment start date """

    df["bdstartdate"] = pymnt_start(df)

    """ consolidating amt paid to date with edge cases """

    df["AmtPaidTodatehack"] = amt_paid(df)

    """ calculating loanBalance to date """

    df["loanBalance"] = df[["AmtOwedAdj", "AmtPaidTodatehack"]].apply(
        loan_balance, axis=1
    )

    log.info(
        "loan balance calculation complete, "
        f"{df.shape[0]} rows, {df.shape[1]} columns"
    )

    """ filtering out any edge cases with data errors and selecting currently active loans """

    df = select_df(df, today)

    log.info(
        "edge case filtering complete, " f"{df.shape[0]} rows, {df.shape[1]} columns"
    )

    """ special calculations for loans with a certain transaction type to exclude it """

    query = """select LoanID from reporting.vw_AdjustmentTransactions_transactions vtat where TransID = 34"""

    loan_convert = pd.read_sql(query, db_connection)

    loan_convert = pd.merge(df, loan_convert, how="inner", on="LoanID")

    loan_convert_id = loan_convert["LoanID"].to_list()

    loan_convert_bank = []
    for loan_id in loan_convert_id:
        bankquery = (
            f"select LoanID, AmtPaid, AmtCharged "
            f"from reporting.vw_BankTransactions_transactions where Transdate <= '{today}' "
            f"and LoanID = '{loan_id}';"
        )
        loan_convert_df = pd.read_sql(bankquery, db_connection)
        loan_convert_bank.append(loan_convert_df)

    loan_convert_bank = pd.concat(loan_convert_bank)

    loan_convert_bank = get_paid_bank(loan_convert_bank)

    log.info(
        "loan convert transactions complete, "
        f"{loan_convert_bank.shape[0]} rows, {loan_convert_bank.shape[1]} columns"
    )

    loan_convert_adj = []
    for loan_id in loan_convert_id:
        adjquery = (
            f"select BusinessID, LoanID, OwedAmtAmtPaid, OwedAmtAmtCharged, TransID, "
            f" dense_rank() over (partition by BusinessID order by LoanID desc) rnk"
            f" from reporting.vw_AdjustmentTransactions_transactions where Transdate <= '{today}' "
            f"and LoanID = '{loan_id}' and TransID != 91 and TransID != 97"
            " and TransID != 34;"
        )
        loan_convert_df = pd.read_sql(adjquery, db_connection)
        loan_convert_adj.append(loan_convert_df)

    loan_convert_adj = pd.concat(loan_convert_adj)

    loan_convert_adj = get_paid_adj(loan_convert_adj)

    log.info(
        "loan convert transactions complete, "
        f"{loan_convert_adj.shape[0]} rows, {loan_convert_adj.shape[1]} columns"
    )

    loan_convert_agg = get_trn_agg(loan_convert_bank, loan_convert_adj)

    log.info(
        "loan convert aggregation complete, "
        f"{loan_convert_agg.shape[0]} rows, {loan_convert_agg.shape[1]} columns"
    )

    df = pd.merge(df, loan_convert_agg, how="left", on="LoanID")

    log.info("dataset merge complete, " f"{df.shape[0]} rows, {df.shape[1]} columns")

    df["AmtPaidTodatehack"] = df["AmtPaidTodate_y"].fillna(df["AmtPaidTodatehack"])

    """ getting business days and weeks passed for loans """

    df["PaymentSchedule"] = df["PaymentSchedule"].str.lower()

    df = time_passed(df, today)

    log.info(
        "business day and weeks passed calculations complete, "
        f"{df.shape[0]} rows, {df.shape[1]} columns"
    )

    """ calculating payments delinquent
        correcting for expected amt exceeds AmtOwedAdj """

    df["ExpectedAmt"] = df[
        ["DailyPayment", "PaymentSchedule", "weekspassed", "dayspassed"]
    ].apply(expectedamt, axis=1)

    df = expectedamt_cleaning(df, today)

    df["AmtDeliquent"] = delinquent_amount(df)

    df["pymnts_delinquent"] = df[["AmtDelinquent", "DailyPayment"]].apply(
        get_pymnts_delinquent, axis=1
    )

    log.info(
        "payments delinquent calculations complete, "
        f"{df.shape[0]} rows, {df.shape[1]} columns"
    )

    """ conversion of payments behind (business days behind) to minutes and getting decimal value
        for calendar days behind """

    df["CalDays"] = get_caldays(df, today)

    log.info(
        "initial calendar day calculations complete, "
        f"{df.shape[0]} rows, {df.shape[1]} columns"
    )

    """ creating a flag where if payments are only delinquent in the week of analysis, then
        calendar days behind = payments delinquent and calendar days cleaning """

    df["CalDays"] = clean_caldays(df, today)

    log.info(
        "calendar days cleaning complete, " f"{df.shape[0]} rows, {df.shape[1]} columns"
    )

    """
    alternative method for calculating calendar days-- faster but less accurate

    df['CalDays'] = np.where(df['pymnts_delinquent']<5, df['pymnts_delinquent'],
                             np.floor(df['pymnts_delinquent']/5)*7 + (df['pymnts_delinquent']%5))
    """

    """ getting payment information """

    df = pyment_info(df)

    log.info(
        "payment calculations complete, " f"{df.shape[0]} rows, {df.shape[1]} columns"
    )

    """ binning """

    df = bins(df)

    log.info("binning complete, " f"{df.shape[0]} rows, {df.shape[1]} columns")

    """ calculating remaining loan_term """

    df = get_remain_loan_term(df, today)

    log.info(
        "remaining loan_term calculation complete, "
        f"{df.shape[0]} rows, {df.shape[1]} columns"
    )

    cleardate = date_var(upload_type)

    """ creating date of analysis variable for record keeping as data becomes historical """

    df["date"] = cleardate

    """ removing extraneous columns """

    df = remove_and_rename(df)

    df = data_cleaning(df)

    log.info(
        "data processing complete complete, "
        f"{df.shape[0]} rows, {df.shape[1]} columns"
    )

    """ uploading data """

    if upload_type == "daily":
        table = "delinquency_daily"
    elif upload_type == "historical":
        table = "delinquency_historical"
    else:
        raise ValueError("Table may not be undefined")

    log.info(f"{upload_type}")

    log.info(f"{table}")

    if upload_type == "daily":
        model = Delinquent_Daily
    elif upload_type == "historical":
        model = Delinquent_Historical
    else:
        raise ValueError("Model may not be undefined")

    log.info(f"{model}")

    """ deleting rows for date of analysis if present """

    db2_session.query(model).filter(model.date == cleardate).delete()
    db2_session.commit()

    log.info("deleted today's data if present")

    log.info(f"attempting to insert {len(df)} records")
    utl.load_df_to_table(
        db2_session, df, table, schema="reports", column_name_list=list(df)
    )
    log.info(f"inserted {len(df)} records")


if __name__ == "__main__":
    logger = get_logger()
    log = logger.new(download_date=datetime.utcnow().isoformat())
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--upload-type")
    args = parser.parse_args()
    conf = config.Config()
    db2_engine = sq.create_engine(conf.db2_connect_str)
    WHSession = sessionmaker(bind=db2_engine)
    wh_session = WHSession()
    db_engine = sq.create_engine(conf.db_connect_str)
    log.info(f"{args.upload_type}")
    get_df(db_engine, wh_session, args.upload_type)
