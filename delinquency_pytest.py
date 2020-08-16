import pandas as pd
import numpy as np
from reporting.source.delinquency import delinquency_project as dd
from datetime import datetime


def test_aggregate():
    df = pd.DataFrame.from_dict(
        {"LoanID": [1, 2, 2], "credit": [100, 200, 400], "debit": [10, 30, 20]}
    )
    df = dd.aggregate(df, "credit", "debit", "amt")
    assert df["amt"].sum() == 640


def test_get_business_days():
    df = pd.DataFrame.from_dict(
        {
            "LoanID": [1, 2, 3],
            "PaymentSchedule": ["daily", "weekly", "daily"],
            "bdstartdate": [
                datetime(2019, 12, 19, 0, 0, 0),
                datetime(2019, 12, 15, 0, 0, 0),
                datetime(2019, 12, 20, 0, 0, 0),
            ],
        }
    )
    date = pd.to_datetime("2019-12-31")
    df["dayspassed"] = df[["PaymentSchedule", "bdstartdate"]].apply(
        dd.get_business_days, args=[date], axis=1
    )
    assert df.dayspassed.isnull().sum() == 1
    assert df.dayspassed.sum() == 15


def test_weeks_between():
    start_date = pd.to_datetime("2020-01-01")
    end_date = pd.to_datetime("2020-01-31")
    n = dd.weeks_between(start_date, end_date)
    assert n == 5


def test_get_weeks():
    today = datetime(2020, 1, 17, 0, 0, 0)
    df = pd.DataFrame.from_dict(
        {
            "PaymentSchedule": ["weekly", "daily", "weekly"],
            "bdstartdate": [
                datetime(2020, 1, 10, 0, 0, 0),
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2020, 1, 1, 0, 0, 0),
            ],
        }
    )

    df["weekspassed"] = df[["PaymentSchedule", "bdstartdate"]].apply(
        dd.get_weeks, args=[today], axis=1
    )
    assert df.weekspassed.isnull().sum() == 1
    assert df.weekspassed.sum() == 5


def test_date_by_subtracting_business_minutes():
    date = pd.to_datetime("2020-01-03")
    n = dd.date_by_subtracting_business_minutes(2880, date)
    value = pd.to_datetime("2020-01-01")
    assert n == value


def test_expected_amt():
    df = pd.DataFrame.from_dict(
        {
            "DailyPayment": [0, 100, 200],
            "PaymentSchedule": ["daily", "weekly", "daily"],
            "weekspassed": [2, 3, 2],
            "dayspassed": [10, 20, 10],
        }
    )
    df["ExpectedAmt"] = df[
        ["DailyPayment", "PaymentSchedule", "weekspassed", "dayspassed"]
    ].apply(dd.expectedamt, axis=1)
    assert df.ExpectedAmt.isnull().sum() == 1
    assert df.ExpectedAmt.sum() == 3500


def test_merge_data():
    df1 = pd.DataFrame.from_dict({"LoanID": [1, 2, 3, 4], "x": [5, 6, 7, 8]})
    df2 = pd.DataFrame.from_dict({"LoanID": [1, 2, 3], "y": [5, 6, 7]})
    df3 = pd.DataFrame.from_dict({"LoanID": [1, 2, 4], "z": [5, 6, 8]})
    df = dd.merge_data(df1, df2, df3)
    assert len(df) == 4


def test_pymnt_start():
    df = pd.DataFrame.from_dict(
        {
            "firstBankTransactionsDate": [np.nan, "2020-03-04"],
            "FirstTrans": [
                datetime(2020, 3, 6, 0, 0, 0),
                datetime(2020, 3, 8, 0, 0, 0),
            ],
        }
    )
    df["bdstartdate"] = dd.pymnt_start(df)
    assert df.bdstartdate.isnull().sum() == 0


def test_amt_paid():
    df = pd.DataFrame.from_dict({"AmtPaidToDate": [10, 20, np.nan]})
    df["amtpaid"] = dd.amt_paid(df)
    assert df.amtpaid.isnull().sum() == 0
    assert df.amtpaid.sum() == 30


def test_select_df():
    date = pd.to_datetime("2020-01-31")
    df = pd.DataFrame.from_dict(
        {
            "LastTrans": [
                datetime(2020, 1, 15, 0, 0, 0),
                datetime(2021, 2, 15, 0, 0, 0),
                datetime(2021, 2, 15, 0, 0, 0),
                datetime(2021, 2, 15, 0, 0, 0),
                datetime(2021, 2, 15, 0, 0, 0),
            ],
            "LoanDate": [
                datetime(2019, 7, 13, 0, 0, 0),
                datetime(2020, 2, 1, 0, 0, 0),
                datetime(2019, 7, 13, 0, 0, 0),
                datetime(2019, 7, 13, 0, 0, 0),
                datetime(2019, 7, 13, 0, 0, 0),
            ],
            "LoanBalance": [12, 32, 0, 4, 12],
            "DailyPayment": [100, 200, 300, 400, 0],
        }
    )
    df = dd.select_df(df, date)
    assert len(df) == 2


def test_time_passed():
    date = pd.to_datetime("2020-01-31")
    df = pd.DataFrame.from_dict(
        {
            "PaymentSchedule": [
                "daily",
                "daily",
                "daily",
                "weekly",
                "weekly",
                "weekly",
            ],
            "bdstartdate": [
                datetime(2020, 1, 21, 0, 0, 0),
                datetime(2008, 2, 13, 0, 0),
                datetime(2020, 2, 13, 0, 0, 0),
                datetime(2020, 1, 21, 0, 0, 0),
                datetime(2008, 2, 13, 0, 0),
                datetime(2020, 2, 13, 0, 0, 0),
            ],
            "firstBankTransactionsDate": [
                datetime(2020, 1, 21, 0, 0, 0),
                datetime(2008, 2, 13, 0, 0),
                datetime(2020, 2, 13, 0, 0, 0),
                datetime(2020, 1, 21, 0, 0, 0),
                datetime(2008, 2, 13, 0, 0),
                datetime(2020, 2, 13, 0, 0, 0),
            ],
        }
    )
    df = dd.time_passed(df, date)
    assert df.dayspassed.sum() == 9
    assert df.weekspassed.sum() == 2


def test_amt_delinquent():
    df = pd.DataFrame.from_dict(
        {
            "ExpectedAmt": [20, 30, 40, 50],
            "AdjAmtOwed": [10, 40, 50, 60],
            "AmtPaidToDatehack": [5, 10, 70, 10],
        }
    )
    df["AmtDelinquent"] = dd.amt_delinquent(df)
    assert df.AmtDelinquent.sum() == 65


def test_get_caldays():
    date = pd.to_datetime("2020-01-21")
    df = pd.DataFrame.from_dict({"LoanID": [1, 2], "pymnts_delinquent": [3, 0]})
    df["CalDays"] = dd.get_caldays(df, date)
    assert df.CalDays.sum() == 5


def test_term_calc():
    df = pd.DataFrame.from_dict(
        {"calcTerm": [2, 3, 4], "term_diff": [3, 2, 1], "neg_months_convert": [1, 1, 1]}
    )
    df["remaining_term"] = df[["calcTerm", "term_diff", "neg_months_convert"]].apply(
        dd.term_calc, axis=1
    )
    assert df.remaining_term.sum() == 5


def test_term_diff():
    date = pd.to_datetime("2020-01-31")
    df = pd.DataFrame.from_dict(
        {
            "LoanID": [1, 2],
            "LoanDate": [
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2019, 12, 1, 0, 0),
            ],
        }
    )
    df["term_diff"] = df.LoanDate.apply(dd.term_diff, args=(date,))
    assert df.term_diff.round().sum() == 3


def test_days_in_last_week():
    date = pd.to_datetime("2020-05-14")
    df = pd.DataFrame.from_dict({"LoanID": [1, 2, 3]})
    df = df.apply(dd.days_in_last_week, args=[date], axis=1)
    assert df.daysinlastweek.sum() == 12


def test_get_remain_term():
    date = pd.to_datetime("2020-01-31")
    df = pd.DataFrame.from_dict(
        {
            "LoanDate": [
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2019, 12, 1, 0, 0, 0),
                datetime(2019, 11, 1, 0, 0),
            ],
            "AdjAmtOwed": [100, 200, 300],
            "AmtPaidToDatehack": [60, 20, 30],
            "DailyPayment": [20, 30, 40],
            "calcTerm": [2, 4, 1],
        }
    )
    df = dd.get_remain_term(df, date)
    assert df.remaining_term.sum().round() == 3


def test_clean_caldays():
    date = pd.to_datetime("2020-05-14")
    df = pd.DataFrame.from_dict(
        {
            "pymnts_delinquent": [3, 12, 6, 0.00001],
            "CalDays": [4, 18, 7, 1],
            "minutesbehind": [300, 400, 1, 20],
        }
    )
    df["CalDays"] = dd.clean_caldays(df, date)
    assert df["CalDays"].sum() == 21


def test_pyment_info():
    df = pd.DataFrame.from_dict(
        {
            "dayspassed": [10, np.nan, 15],
            "weekspassed": [np.nan, 5, np.nan],
            "PaymentSchedule": ["daily", "weekly", "daily"],
            "AmtPaidToDatehack": [100, 200, 300],
            "DailyPayment": [10, 20, 30],
        }
    )
    df = dd.pyment_info(df)
    assert df["pymnts_expected"].sum() == 30
    assert df["pymnts_paid"].sum() == 22


def test_bins_nulls():
    df = pd.DataFrame.from_dict(
        {
            "CalDays": [2, 6, 18, np.nan, np.inf],
            "Revenue": [np.nan, 5000, 300000, 200000, 510000],
            "YearsInBusiness": [2, 5, np.nan, 15, 23],
            "CreditScore": [425, 525, 619, np.nan, 551],
            "CreditScore2": [425, np.nan, 619, 475, 551],
        }
    )
    df = dd.bins(df)
    assert df.DelinquencyBins.isnull().sum() == 2
    assert df.CreditScorebins.isnull().sum() == 1
    assert df.CreditScore2bins.isnull().sum() == 1


def test_bins_values():
    df = pd.DataFrame.from_dict(
        {
            "CalDays": [2, 6, 18],
            "Revenue": [5000, 300000, 140000],
            "YearsInBusiness": [2, 5, 23],
            "CreditScore": [425, 525, 619],
            "CreditScore2": [619, 475, 551],
        }
    )
    df = dd.bins(df)
    assert all(df.DelinquencyBins.values == ["0-3", "4-14", "15-29"])
    assert all(df.RevenueBins.values == ["0-119K", "200-499K", "120-174K"])
    assert all(df.RevenueBins2.values == ["0-174K", "200K+", "0-174K"])
    assert all(df.YIBbins.values == ["2", "5", "20-24"])
    assert all(df.CreditScorebins.values == ["400-449", "500-549", "600-649"])
    assert all(df.CreditScore2bins.values == ["600-649", "450-499", "550-599"])


def test_transform_active_Loans():
    df = pd.DataFrame.from_dict(
        {
            "LastTrans": ["2020-01-19", np.nan, "2019-08-15"],
            "FirstTrans": ["2019-08-14", np.nan, "2019-05-13"],
            "rnkactive": [1, 0, 0],
            "AmtOwedBalanceToFwd": [100, 200, 300],
            "LoanAmtOwed": [300, 400, 500],
            "Fee": [50, 50, 50],
            "FwdedBal": [200, 300, 400],
            "InterestOnRefi": [0, 0, 50],
        }
    )
    df = dd.transform_active_Loans(df)
    assert df.LastTrans.isnull().sum() == 1
    assert df.FirstTrans.isnull().sum() == 1
    assert df.AmtOwedBalanceToFwd.sum() == 500
    assert df.AdjAmtOwed.sum() == 1800


def test_get_paid_bank():
    df = pd.DataFrame.from_dict(
        {"AmtPaid": [10, np.nan, 20], "AmtCharged": [np.nan, 5, 7]}
    )
    df = dd.get_paid_bank(df)
    assert df.AmtPaid.isnull().sum() == 0
    assert df.AmtCharged.isnull().sum() == 0
    assert df.AmtPaid.sum() == 30
    assert df.AmtCharged.sum() == 12


def test_get_paid_adj():
    df = pd.DataFrame.from_dict(
        {
            "AmtOwedAmtPaid": [10, np.nan, 20],
            "AmtOwedAmtCharged": [np.nan, 5, 7],
            "rnk": [0, 0, 0],
            "TransType": [30, 18, 19],
        }
    )
    df = dd.get_paid_adj(df)
    assert df.AmtOwedAmtPaid.isnull().sum() == 0
    assert df.AmtOwedAmtCharged.isnull().sum() == 0
    assert df.AmtOwedAmtPaid.sum() == 30
    assert df.AmtOwedAmtCharged.sum() == 12
    assert len(df) == 3


def test_min_BankTransactions_date():
    df = pd.DataFrame.from_dict(
        {
            "LoanID": [1, 1, 2, 2],
            "TransDate": [
                datetime(2020, 1, 1, 0, 0, 0),
                datetime(2020, 2, 1, 0, 0, 0),
                datetime(2019, 12, 31, 0, 0, 0),
                datetime(2020, 1, 15, 0, 0, 0),
            ],
        }
    )
    df = dd.min_BankTransactions_date(df)
    df["firstBankTransactionsDate"] = df["firstBankTransactionsDate"].astype(str)
    assert all(df.firstBankTransactionsDate.values == ["2020-01-01", "2019-12-31"])


def test_get_trn_agg():
    bank = pd.DataFrame.from_dict(
        {
            "LoanID": [1, 1, 2, 2, 3, 3],
            "AmtPaid": [100, 200, np.nan, np.nan, 300, 400],
            "AmtCharged": [10, 20, np.nan, np.nan, 30, 40],
        }
    )
    adj = pd.DataFrame.from_dict(
        {
            "LoanID": [1, 1, 2, 2, 3, 3],
            "AmtOwedAmtPaid": [np.nan, np.nan, 100, 200, 300, 400],
            "AmtOwedAmtCharged": [np.nan, np.nan, 10, 20, 30, 40],
        }
    )
    df = dd.get_trn_agg(bank, adj)
    assert df["AmtPaidToDate"].isnull().sum() == 0
    assert df["AmtPaidToDate"].sum() == 1800
    assert len(df) == 3


def test_expectedamt_cleaning():
    df = pd.DataFrame.from_dict(
        {
            "LoanID": [1, 2, 3, 4],
            "dayspassed": [5, 10, 15, 20],
            "ExpectedAmt": [1000, 2000, 3000, 4000],
            "AdjAmtOwed": [500, 5000, 8000, 9000],
            "AmtPaidToDatehack": [400, 0, 3000, 3800],
        }
    )
    today = pd.to_datetime("2020-08-14")
    df = dd.expectedamt_cleaning(df, today)
    assert df["ExpectedAmt"].sum() == 9500


def test_data_cleaning():
    df = pd.DataFrame.from_dict(
        {
            "caldays": [np.inf, 5, 3, 10],
            "payments_delinquent": [2, np.inf, 4, 8],
            "payments_expected": [5, 4, np.inf, 10],
            "payments_paid": [3, 2, np.inf, 9],
        }
    )
    df = dd.data_cleaning(df)
    assert df["caldays"].isnull().sum() == 1
    assert df["payments_delinquent"].isnull().sum() == 1
    assert df["payments_expected"].isnull().sum() == 1
    assert df["payments_paid"].isnull().sum() == 1
    assert df["caldays"].sum() == 18
    assert df["payments_delinquent"].sum() == 14
    assert df["payments_expected"].sum() == 19
    assert df["payments_paid"].sum() == 14


def test_remove_and_rename():
    df1 = pd.DataFrame.from_dict(
        {
            "LoanID": [1],
            "BusinessID": [1],
            "LoanDate": [1],
            "FirstTrans": [1],
            "firstBankTransactionsDate": [1],
            "LoanAmt": [1],
            "calcTerm": [1],
            "LoanAmtOwed": [1],
            "FwdedBal": [1],
            "Fee": [1],
            "AmtOwedBalanceToFwd": [1],
            "AdjAmtOwed": [1],
            "AmtPaidToDatehack": [1],
            "LoanBalance": [1],
            "DailyPayment": [1],
            "dayspassed": [1],
            "weekspassed": [1],
            "ExpectedAmt": [1],
            "AmtDelinquent": [1],
            "CalDays": [1],
            "pymnts_delinquent": [1],
            "PaymentSchedule": [1],
            "DelinquencyBins": [1],
            "RevenueBins": [1],
            "RevenueBins2": [1],
            "YIBbins": [1],
            "CreditScorebins": [1],
            "CreditScore2bins": [1],
            "CreditScore": [1],
            "CreditScore2": [1],
            "YearsInBusiness": [1],
            "Revenue": [1],
            "AppType": [1],
            "Industry1": [1],
            "Industry2": [1],
            "Industry3": [1],
            "Industry4": [1],
            "RepType": [1],
            "date": [1],
            "remaining_term": [1],
            "pymnts_expected": [1],
            "pymnts_paid": [1],
            "x": [1],
            "y": [1],
            "z": [1],
        }
    )
    df2 = pd.DataFrame.from_dict(
        {
            "Loan_id": [1],
            "first_contractid": [1],
            "Loan_date": [1],
            "first_deposit": [1],
            "first_BankTransactions_date": [1],
            "Loan_amt": [1],
            "term": [1],
            "Loan_AmtOwed": [1],
            "balance_fwd": [1],
            "refinance_fee": [1],
            "AmtOwed_balance_tofwd": [1],
            "adjAmtOwed": [1],
            "amt_paid_todate": [1],
            "Loan_balance": [1],
            "payment_amt": [1],
            "days_passed": [1],
            "weeks_passed": [1],
            "expected_amt": [1],
            "amt_deliquent": [1],
            "caldays": [1],
            "payments_delinquent": [1],
            "offer_accepted_type": [1],
            "delinquency_bins": [1],
            "annual_sales_bins": [1],
            "annual_sales_bins2": [1],
            "yib_bins": [1],
            "CreditScore_bins": [1],
            "CreditScore2_bins": [1],
            "CreditScore": [1],
            "CreditScore2": [1],
            "years_owned_business": [1],
            "annual_sales": [1],
            "app_type": [1],
            "Industry1": [1],
            "Industry2": [1],
            "Industry3": [1],
            "Industry4": [1],
            "rep_type": [1],
            "date": [1],
            "remaining_term": [1],
            "payments_expected": [1],
            "payments_paid": [1],
        }
    )
    df1 = dd.remove_and_rename(df1)
    assert df1.equals(df2)


def test_Loan_balance():
    df = pd.DataFrame.from_dict(
        {"AdjAmtOwed": [1000, 2000, 3000], "AmtPaidToDatehack": [500, 0, 1500]}
    )
    df["LoanBalance"] = df[["AdjAmtOwed", "AmtPaidToDatehack"]].apply(
        dd.Loan_balance, axis=1
    )
    assert df["LoanBalance"].sum() == 4000


def test_get_pymnts_delinquent():
    df = pd.DataFrame.from_dict(
        {"AmtDelinquent": [100, 200, 300], "DailyPayment": [10, np.nan, 30]}
    )
    df["pymnts_delinquent"] = df[["AmtDelinquent", "DailyPayment"]].apply(
        dd.get_pymnts_delinquent, axis=1
    )
    assert df["pymnts_delinquent"].sum() == 20


def test_date_var():
    daily = "daily"
    x = dd.date_var(daily)
    assert x == dd.get_today()
    historical = "historical"
    y = dd.date_var(historical)
    assert y == dd.get_date()


def test_modified_pymnt_1215888():
    df = pd.DataFrame.from_dict(
        {
            "LoanID": [1215888, 1215888, 1215888, 2],
            "dayspassed": [20, 126, 150, 10],
            "ExpectedAmt": [5, 100, 24, 500],
        }
    )
    df["ExpectedAmt"] = np.where(
        df["LoanID"] == 1215888,
        df.apply(dd.modified_pymnt_1215888, axis=1),
        df["ExpectedAmt"],
    )
    assert df["ExpectedAmt"].sum() == 52385.84
