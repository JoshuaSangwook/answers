-- Question A
   Select userGroup,experiment,count(*)
    From ExperimentLog
Group by userGroup,experiment
;

-- Question B
Select date,experiment, cnt, date, row_number() over (partition by date,experiment order  by cnt desc ) num
From (
    Select date,experiment,count(*) as cnt
      From ExperimentLog
  Group by date,experiment ) a
Where num = 1
;

