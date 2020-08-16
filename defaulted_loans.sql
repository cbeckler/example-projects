create PROCEDURE [reporting].[write_off_loans]

AS
BEGIN
	SET NOCOUNT ON;


/* getting maximum loan date by contract */

SELECT
	BusinessContract.BusinessID ,
	MAX(Loans.LoanDate) AS MaxLoanDate
into
	#MaxLoanDate
FROM
	BusinessContract
INNER JOIN
	Loans
ON
	BusinessContract.BusinessID = Loans.BusinessID
GROUP BY
	BusinessContract.BusinessID


/* getting writeoff amount for defaulted loans
and joining it to most recent loan by contract */

SELECT
	SUM(ROUND(AdjustedTransactions.PaidAmt, 2)) AS WriteOffAmt,
	BusinessContract.BusinessID,
	BusinessContract.BusAcctID,
	Loans.LoanID,
	#MaxLoanDate.MaxLoanDate,
	1 as WriteOff
into
	#WriteOff
FROM
	BusinessContract
INNER JOIN
	AdjustedTransactions
ON
	BusinessContract.BusinessID = AdjustedTransactions.BusinessID
INNER JOIN
	#MaxLoanDate
ON
	BusinessContract.BusinessID = #MaxLoanDate.BusinessID
INNER JOIN
	Loans
ON
	#MaxLoanDate.BusinessID = Loans.BusinessID
	AND #MaxLoanDate.MaxLoanDate = Loans.LoanDate
WHERE
	(AdjustedTransactions.TransType = 81)
GROUP BY
	BusinessContract.BusinessName,
	BusinessContract.BusinessID,
	BusinessContract.BusAcctID,
	Loans.LoanID,
	#MaxLoanDate.MaxLoanDate
HAVING
	(SUM(ROUND(AdjustedTransactions.PaidAmt, 2)) <> 0)




select
	f.LoanID,
	f.BusinessID,
	f.LoanDate,
	f.LoanAmt,
	f.LoanOwedAmt,
	f.LoanRefiFee,
	f.FowardedBalance,
	f.DailyPaymentAmt,
	f.LoanTerm,
	f.BalanceForwarded,
	case when f.RefinanceInterest is null then 0 else f.RefinanceInterest end InterestOnRefinance
into
	#Loans2
FROM
	Loans f




select
	f.LoanID,
	f.LoanDate,
	f.LoanAmt,
	(f.LoanOwedAmt + f.LoanRefiFee + f.FowardedBalance + InterestOnRefinance - f.BalanceForwarded) as AdjustedOwedAmt,
	w.WriteOff,
	w.WriteOffAmt,
	CASE
 		WHEN cw.CreditScore_2 IS NULL
 			THEN cw.CreditScore_1
 		WHEN cw.CreditScore_1 IS NULL AND cw.CreditScore_2 IS NOT NULL
 			THEN cw.CreditScore_2
 		ELSE
 			(cw.CreditScore_1 + cw.CreditScore_2)/2
 	END AvgCreditScore /* getting average credit score where appropriate and consolidating scores into one value */,
 	concat(s.Industry1, ' - ', s.Industry1Division) Industry1,
 	concat(s.Industry2, ' - ', s.Industry2MajorGroup) Industry2,
 	/* Industry descriptions */
 	case
 		when f.DailyPaymentAmt = 0
 			then f.LoanTerm
 		when beginningAmtOwed < 0
 			then f.LoanTerm
 		else (beginningAmtOwed/f.DailyPaymentAmt)/21
 	end Term /* calculating more accurate term with DailyPaymentAmt */
into
	#Loansjoin
from
	#Loans2 f
inner join
	Loans_beginning_owed b
on
	f.LoanID = b.LoanID
left join
	#WriteOff w
on
	f.LoanDate = w.MaxLoanDate
	and f.BusinessID = w.BusinessID
inner join
	BusinessContract c
on
	f.BusinessID = c.BusinessID
inner join
	BusinessAccount ac
on
	c.BusAcctID = ac.BusAcctID
inner join
	Prospectives l
on
	ac.ProspectiveID = l.ProspectiveID
left join
	Industry s
on
	l.IndustryID = s.Industry4
LEFT JOIN
	credit cw
ON
	f.LoanID = cw.LoanID


select
	LoanID,
	LoanDate,
	LoanAmt,
	AdjustedOwedAmt,
	WriteOff,
	WriteOffAmt,
	Industry1,
	Industry2,
	ceiling(Term) as TermNoBin /* rouding up term */,
	CASE
		WHEN AvgCreditScore <= 400 AND AvgCreditScore < 450
			THEN '400-449'
		WHEN AvgCreditScore <= 450 AND AvgCreditScore < 500
			THEN '450-499'
		WHEN AvgCreditScore <= 500 AND AvgCreditScore < 550
			THEN '500-549'
		WHEN AvgCreditScore <= 550 AND AvgCreditScore < 600
			THEN '550-599'
		WHEN AvgCreditScore  <= 600 AND AvgCreditScore < 650
			THEN '600-649'
		WHEN AvgCreditScore  <= 650 AND AvgCreditScore < 700
			THEN '650-699'
		WHEN AvgCreditScore  <= 700 AND AvgCreditScore < 750
			THEN '700-749'
		WHEN AvgCreditScore <= 750 AND AvgCreditScore < 800
			THEN '750-799'
		WHEN AvgCreditScore <= 800 AND AvgCreditScore < 850
			THEN '800-849'
		WHEN AvgCreditScore <= 850 AND AvgCreditScore < 900
			THEN '850-899'
		ELSE NULL
	END CreditBins,
	CASE
		WHEN AvgCreditScore <= 400 AND AvgCreditScore < 450
			THEN 1
		WHEN AvgCreditScore <= 450 AND AvgCreditScore < 500
			THEN 2
		WHEN AvgCreditScore <= 500 AND AvgCreditScore < 550
			THEN 3
		WHEN AvgCreditScore <= 550 AND AvgCreditScore < 600
			THEN 4
		WHEN AvgCreditScore  <= 600 AND AvgCreditScore < 650
			THEN 5
		WHEN AvgCreditScore <= 650 AND AvgCreditScore < 700
			THEN 6
		WHEN AvgCreditScore <= 700 AND AvgCreditScore < 750
			THEN 7
		WHEN AvgCreditScore <= 750 AND AvgCreditScore < 800
			THEN 8
		WHEN AvgCreditScore <= 800 AND AvgCreditScore < 850
			THEN 9
		WHEN AvgCreditScore <= 850 AND AvgCreditScore < 900
			THEN 10
		ELSE NULL
	END creditrank /* ranking FICO bins for power bi sorting */,
	CASE
		WHEN LoanAmt >= 0 AND LoanAmt < 50000
			THEN '$0-49K'
		WHEN LoanAmt >= 50000 AND LoanAmt < 100000
			THEN '$50-99K'
		WHEN LoanAmt >= 100000 AND LoanAmt < 150000
			THEN '$100-149K'
		WHEN LoanAmt >= 150000 AND LoanAmt < 200000
			THEN '$150-199K'
		WHEN LoanAmt >= 200000 AND LoanAmt < 250000
			THEN '$200-249K'
		WHEN LoanAmt >= 250000 AND LoanAmt < 300000
			THEN '$250-299K'
		WHEN LoanAmt >= 300000 AND LoanAmt < 350000
			THEN '$300-349K'
		WHEN LoanAmt >= 350000 AND LoanAmt < 400000
			THEN '$350-399K'
		WHEN LoanAmt >= 400000 AND LoanAmt < 450000
			THEN '$400-449K'
		WHEN LoanAmt >= 450000 AND LoanAmt < 500000
			THEN '$450-499K'
		WHEN LoanAmt >= 500000 AND LoanAmt < 550000
			THEN '$500-549K'
		WHEN LoanAmt >= 550000 AND LoanAmt < 600000
			THEN '$550-599K'
		WHEN LoanAmt >= 600000 AND LoanAmt < 650000
			THEN '$600-649K'
		WHEN LoanAmt >= 650000 AND LoanAmt < 700000
			THEN '$650-699K'
		WHEN LoanAmt >= 700000 AND LoanAmt < 750000
			THEN '$700-749K'
		WHEN LoanAmt >= 750000 AND LoanAmt < 800000
			THEN '$750-799K'
		WHEN LoanAmt >= 800000 AND LoanAmt < 850000
			THEN '$800-849K'
		ELSE NULL
	END LoanAmtBin,
	CASE
		WHEN LoanAmt >= 0 AND LoanAmt < 50000
			THEN 17
		WHEN LoanAmt >= 50000 AND LoanAmt < 100000
			THEN 16
		WHEN LoanAmt >= 100000 AND LoanAmt < 150000
			THEN 15
		WHEN LoanAmt >= 150000 AND LoanAmt < 200000
			THEN 14
		WHEN LoanAmt >= 200000 AND LoanAmt < 250000
			THEN 13
		WHEN LoanAmt >= 250000 AND LoanAmt < 300000
			THEN 12
		WHEN LoanAmt >= 300000 AND LoanAmt < 350000
			THEN 11
		WHEN LoanAmt >= 350000 AND LoanAmt < 400000
			THEN 10
		WHEN LoanAmt >= 400000 AND LoanAmt < 450000
			THEN 9
		WHEN LoanAmt >= 450000 AND LoanAmt < 500000
			THEN 8
		WHEN LoanAmt >= 500000 AND LoanAmt < 550000
			THEN 7
		WHEN LoanAmt >= 550000 AND LoanAmt < 600000
			THEN 6
		WHEN LoanAmt >= 600000 AND LoanAmt < 650000
			THEN 5
		WHEN LoanAmt >= 650000 AND LoanAmt < 700000
			THEN 4
		WHEN LoanAmt >= 700000 AND LoanAmt < 750000
			THEN 3
		WHEN LoanAmt >= 750000 AND LoanAmt < 800000
			THEN 2
		WHEN LoanAmt >= 800000 AND LoanAmt < 850000
			THEN 1
		ELSE NULL
	END LoanAmtRnk /* ranking funding bins for power bi sorting */,
	case
		when ceiling(Term) >= 0 and ceiling(Term) < 4
			then '1-3'
		when ceiling(Term) >= 4 and ceiling(Term) < 7
			then '4-6'
		when ceiling(Term) >= 7 and ceiling(Term) < 10
			then '7-9'
		when ceiling(Term) >= 10 and ceiling(Term) < 13
			then '10-12'
		when ceiling(Term) >= 13 and ceiling(Term) < 16
			then '13-15'
		when ceiling(Term) >= 16 and ceiling(Term) < 19
			then '16-18'
		when ceiling(Term) > 19
			then '>18'
	end Term /* term bins */,
	case
		when ceiling(Term) >= 0 and ceiling(Term) < 4
			then 1
		when ceiling(Term) >= 4 and ceiling(Term) < 7
			then 2
		when ceiling(Term) >= 7 and ceiling(Term) < 10
			then 3
		when ceiling(Term) >= 10 and ceiling(Term) < 13
			then 4
		when ceiling(Term) >= 13 and ceiling(Term) < 16
			then 5
		when ceiling(Term) >= 16 and ceiling(Term) < 19
			then 6
		when ceiling(Term) > 19
			then 7
	end termrank /* ranking term bins for power bi sorting */,
	month(LoanDate) as monthrank /* getting month number for power bi sorting */
from
	#Loansjoin


end;
