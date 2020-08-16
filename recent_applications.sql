create view reports.Apps_daily as
with x as
(select
	a.AppID,
	Industry2
from
	Apps a
inner join
	Accts ac
on
	a.AcctID = ac.AcctID
inner join
	Industry
on
	ac.IndustryID = Industry.Industry4
where
	ApplicationDate >= '1/1/2018')
/* getting each application's industry */
,x2 as
(select
	Industry2,
	count(*) num
from
	x
group by
	Industry2)
,x3 as
(select
	top 10 Industry2
from
	x2
where
	Industry2 in (select top 10 Industry2 from x2 order by num desc))
/* getting top 10 industries for all time */
,lastday as
(select
	a.AppID,
	Industry2
from
	Apps a
inner join
	Accts ac
on
	a.AcctID = ac.AcctID
inner join
	Industry
on
	ac.IndustryID = Industry.Industry4
where cast(ApplicationDate as date) = dateadd(DD, -1, cast(getdate() as date)))
/* getting applications from previous day */
,lastdaycount as
(select
	Industry2,
	count(*) num
from
	lastday
group by
	Industry2)
,top10 as
(select
	top 10 Industry2
from
	lastdaycount
where
	Industry2 in (select top 10 Industry2 from lastdaycount order by num desc))
/* getting top 10 industries for yesterday */
select
	concat(Industry.Industry2,' - ',Industry.Industry2MajorGroup) as Industry2,
	case
		when Industry.Industry2 in (select Industry2 from x3)
			then 1
		else 0
	end top10IndustryOverall,
	case
		when Industry.Industry2 in (select Industry2 from top10)
			then 1
		else 0
	end top10IndustryLastDay,
	0 as top5Industry,
	a.AppID,
	case
		when a.YearsInBusiness > 400
			then null
		else a.YearsInBusiness
	end YearsInBusiness,
	/* filtering invalid data */
	a.AnnualRevenue,
	case
		when a.App_Type in ('New','Resubmitted')
			then 'New'
		else 'Subsequent'
	end AppType,
	case
		when s.Last_Name = 'Outside'
			then 'Outside'
		else 'InHouse'
	end RepType,
	CASE
 		WHEN CreditScore_2 IS NULL
 			THEN CreditScore_1
 		WHEN CreditScore_1 IS NULL AND CreditScore_2 IS NOT NULL
 			THEN CreditScore_2
 		ELSE
 			(CreditScore_1 + CreditScore_2)/2
 	END AvgCreditScore,
 	case
 		when a.AppStatus IN (5, 8, 11, 24)
 			then 1
 		else 0
 	end AprvCount,
 	f.LoanID,
 	f.LoanAmt,
 	a.ApplicationDate,
 	case
 		when cast(ApplicationDate as date) = (select max(cast(ApplicationDate as date)) from Apps)
 			then 1
 		else 0
 	end istoday,
	/* getting today's applications */
 	case
 		when a.Bankrupt  = 1
 			then 1
 		else 0
 	end Bankrupt,
 	a.ProcessID
from
	Apps a
inner join
	Accts ac
on
	a.AcctID = ac.AcctID
left join
	Prospectives l
on
	ac.ProspectiveID = l.ProspectiveID
left join
	Industry
on
	l.IndustryID = Industry.Industry4
left join
	Representatives s
on
	ac.RepID = s.RepID
left join
	credit c
on
	a.AcctID = c.AcctID
left join
	Loans f
on
	a.LoanID = f.LoanID
where
	ApplicationDate >= '1/1/2018' and upper(ac.BusinessName) != 'TEST';
