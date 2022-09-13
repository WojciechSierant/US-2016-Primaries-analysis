-- preparing tables for importing data
create table county_facts (
fips float(20) ,
area_name varchar(150) ,
state_abbreviation varchar(50) ,
PST045214 float(20) ,
PST040210 float(20) ,
PST120214 float(20) ,
POP010210 float(20) ,
AGE135214 float(20) ,
AGE295214 float(20) ,
AGE775214 float(20) ,
SEX255214 float(20) ,
RHI125214 float(20) ,
RHI225214 float(20) ,
RHI325214 float(20) ,
RHI425214 float(20) ,
RHI525214 float(20) ,
RHI625214 float(20) ,
RHI725214 float(20) ,
RHI825214 float(20) ,
POP715213 float(20) ,
POP645213 float(20) ,
POP815213 float(20) ,
EDU635213 float(20) ,
EDU685213 float(20) ,
VET605213 float(20) ,
LFE305213 float(20) ,
HSG010214 float(20) ,
HSG445213 float(20) ,
HSG096213 float(20) ,
HSG495213 float(20) ,
HSD410213 float(20) ,
HSD310213 float(20) ,
INC910213 float(20) ,
INC110213 float(20) ,
PVY020213 float(20) ,
BZA010213 float(20) ,
BZA110213 float(20) ,
BZA115213 float(20) ,
NES010213 float(20) ,
SBO001207 float(20) ,
SBO315207 float(20) ,
SBO115207 float(20) ,
SBO215207 float(20) ,
SBO515207 float(20) ,
SBO415207 float(20) ,
SBO015207 float(20) ,
MAN450207 float(20) ,
WTN220207 float(20) ,
RTN130207 float(20) ,
RTN131207 float(20) ,
AFN120207 float(20) ,
BPS030214 float(20) ,
LND110210 float(20) ,
POP060210 float(20)
);


create table primary_results (
state varchar(50) ,
state_abbreviation varchar(50) ,
county varchar(50) ,
fips float(20),
party varchar(50) ,
candidate varchar(50) ,
votes integer ,
fraction_votes float(20)
)

--creating summary table that only shows states, stats and dem/rep votes
create table state_summary as (
--total query
with votes as (
--votes
select distinct
p.state_abbreviation sa,
p.party,
sum(p.votes) over (partition by p.state, p.party)::real/sum(p.votes) over (partition by p.state)::real*100 as percent_votes
from primary_results p
order by p.state_abbreviation
),

--calculating average % stats across counties to present as states data
--selecting only stats as % for simplicity
state_data as (
-- county data
SELECT * from weighted_states
)

--removing additional gaps - missing both parities voting data
select
*
from votes v
join weighted_states d
on v.sa = d.state_abbreviation
where v.percent_votes <> 100 and v.party = 'Democrat')


--calculating correlation coefficients stats vs voting across states
select
corr(s.percent_votes,s."Persons under 5 years") "Persons under 5 years",
corr(s.percent_votes,s."Persons under 18 years") "Persons under 18 years",
corr(s.percent_votes,s."Persons 65 years and over") "Persons 65 years and over",
corr(s.percent_votes,s."Female persons") "Female persons",
corr(s.percent_votes,s."White alone") "White alone",
corr(s.percent_votes,s."Black or African American alone") "Black or African American alone",
corr(s.percent_votes,s."American Indian and Alaska Native alone") "American Indian and Alaska Native alone",
corr(s.percent_votes,s."Asian alone") "Asian alone",
corr(s.percent_votes,s."Native Hawaiian and Other Pacific Islander alone") "Native Hawaiian and Other Pacific Islander alone",
corr(s.percent_votes,s."Two or More Races") "Two or More Races",
corr(s.percent_votes,s."Hispanic or Latino") "Hispanic or Latino",
corr(s.percent_votes,s."White alone, not Hispanic or Latino") "White alone, not Hispanic or Latino",
corr(s.percent_votes,s."Living in same house 1 year & over") "Living in same house 1 year & over",
corr(s.percent_votes,s."Foreign born persons") "Foreign born persons",
corr(s.percent_votes,s."High school graduate or higher") "High school graduate or higher",
corr(s.percent_votes,s."Bachelors degree or higher") "Bachelors degree or higher",
corr(s.percent_votes,s."Housing units in multi-unit structures") "Housing units in multi-unit structures",
corr(s.percent_votes,s."Persons below poverty level") "Persons below poverty level",
corr(s.percent_votes,s."Private nonfarm employment") "Private nonfarm employment",
corr(s.percent_votes,s."Black-owned firms") "Black-owned firms",
corr(s.percent_votes,s."American Indian- and Alaska Native-owned firms") "American Indian- and Alaska Native-owned firms",
corr(s.percent_votes,s."Asian-owned firms") "Asian-owned firms",
corr(s.percent_votes,s."Native Hawaiian- and Other Pacific Islander-owned firms") "Native Hawaiian- and Other Pacific Islander-owned firms",
corr(s.percent_votes,s."Hispanic-owned firms") "Hispanic-owned firms",
corr(s.percent_votes,s."Women-owned firms") "Women-owned firms"
from state_summary s

-- selecting only states close to vote parity
select *
from state_summary
where percent_votes between 45 and 55
order by "Hispanic-owned firms" desc

--final weighted % query
create table weighted_states as(
with weighted_percent as (
SELECT distinct
f.state_abbreviation,
f.area_name,
f.PST045214/sum(f.PST045214) over(partition by f.state_abbreviation) as county_ratio,
f.AGE135214*(f.PST045214/sum(f.PST045214) over(partition by f.state_abbreviation)) as "Persons under 5 years",
f.AGE295214*(f.PST045214/sum(f.PST045214) over(partition by f.state_abbreviation)) as "Persons under 18 years",
f.AGE775214*(f.PST045214/sum(f.PST045214) over(partition by f.state_abbreviation)) as "Persons 65 years and over",
f.SEX255214*(f.PST045214/sum(f.PST045214) over(partition by f.state_abbreviation)) as "Female persons",
f.RHI125214*(f.PST045214/sum(f.PST045214) over(partition by f.state_abbreviation)) as "White alone",
f.RHI225214*(f.PST045214/sum(f.PST045214) over(partition by f.state_abbreviation)) as "Black or African American alone",
f.RHI325214*(f.PST045214/sum(f.PST045214) over(partition by f.state_abbreviation)) as "American Indian and Alaska Native alone",
f.RHI425214*(f.PST045214/sum(f.PST045214) over(partition by f.state_abbreviation)) as "Asian alone",
f.RHI525214*(f.PST045214/sum(f.PST045214) over(partition by f.state_abbreviation)) as "Native Hawaiian and Other Pacific Islander alone",
f.RHI625214*(f.PST045214/sum(f.PST045214) over(partition by f.state_abbreviation)) as "Two or More Races",
f.RHI725214*(f.PST045214/sum(f.PST045214) over(partition by f.state_abbreviation)) as "Hispanic or Latino",
f.RHI825214*(f.PST045214/sum(f.PST045214) over(partition by f.state_abbreviation)) as "White alone, not Hispanic or Latino",
f.POP715213*(f.PST045214/sum(f.PST045214) over(partition by f.state_abbreviation)) as "Living in same house 1 year & over",
f.POP645213*(f.PST045214/sum(f.PST045214) over(partition by f.state_abbreviation)) as "Foreign born persons",
f.EDU635213*(f.PST045214/sum(f.PST045214) over(partition by f.state_abbreviation)) as "High school graduate or higher",
f.EDU685213*(f.PST045214/sum(f.PST045214) over(partition by f.state_abbreviation)) as "Bachelors degree or higher",
f.HSG096213*(f.PST045214/sum(f.PST045214) over(partition by f.state_abbreviation)) as "Housing units in multi-unit structures",
f.PVY020213*(f.PST045214/sum(f.PST045214) over(partition by f.state_abbreviation)) as "Persons below poverty level",
f.BZA115213*(f.PST045214/sum(f.PST045214) over(partition by f.state_abbreviation)) as "Private nonfarm employment",
f.SBO315207*(f.PST045214/sum(f.PST045214) over(partition by f.state_abbreviation)) as "Black-owned firms",
f.SBO115207*(f.PST045214/sum(f.PST045214) over(partition by f.state_abbreviation)) as "American Indian- and Alaska Native-owned firms",
f.SBO215207*(f.PST045214/sum(f.PST045214) over(partition by f.state_abbreviation)) as "Asian-owned firms",
f.SBO515207*(f.PST045214/sum(f.PST045214) over(partition by f.state_abbreviation)) as "Native Hawaiian- and Other Pacific Islander-owned firms",
f.SBO415207*(f.PST045214/sum(f.PST045214) over(partition by f.state_abbreviation)) as "Hispanic-owned firms",
f.SBO015207*(f.PST045214/sum(f.PST045214) over(partition by f.state_abbreviation)) as "Women-owned firms"
from county_facts f
order by f.state_abbreviation)

select distinct
wp.state_abbreviation,
sum(wp."Persons under 5 years") over(partition by wp.state_abbreviation) "Persons under 5 years",
sum(wp."Persons under 18 years") over(partition by wp.state_abbreviation) "Persons under 18 years",
sum(wp."Persons 65 years and over") over(partition by wp.state_abbreviation) "Persons 65 years and over",
sum(wp."Female persons") over(partition by wp.state_abbreviation) "Female persons",
sum(wp."White alone") over(partition by wp.state_abbreviation) "White alone",
sum(wp."Black or African American alone") over(partition by wp.state_abbreviation) "Black or African American alone",
sum(wp."American Indian and Alaska Native alone") over(partition by wp.state_abbreviation) "American Indian and Alaska Native alone",
sum(wp."Asian alone") over(partition by wp.state_abbreviation) "Asian alone",
sum(wp."Native Hawaiian and Other Pacific Islander alone") over(partition by wp.state_abbreviation) "Native Hawaiian and Other Pacific Islander alone",
sum(wp."Two or More Races") over(partition by wp.state_abbreviation) "Two or More Races",
sum(wp."Hispanic or Latino") over(partition by wp.state_abbreviation) "Hispanic or Latino",
sum(wp."White alone, not Hispanic or Latino") over(partition by wp.state_abbreviation) "White alone, not Hispanic or Latino",
sum(wp."Living in same house 1 year & over") over(partition by wp.state_abbreviation) "Living in same house 1 year & over",
sum(wp."Foreign born persons") over(partition by wp.state_abbreviation) "Foreign born persons",
sum(wp."High school graduate or higher") over(partition by wp.state_abbreviation) "High school graduate or higher",
sum(wp."Bachelors degree or higher") over(partition by wp.state_abbreviation) "Bachelors degree or higher",
sum(wp."Housing units in multi-unit structures") over(partition by wp.state_abbreviation) "Housing units in multi-unit structures",
sum(wp."Persons below poverty level") over(partition by wp.state_abbreviation) "Persons below poverty level",
sum(wp."Private nonfarm employment") over(partition by wp.state_abbreviation) "Private nonfarm employment",
sum(wp."Black-owned firms") over(partition by wp.state_abbreviation) "Black-owned firms",
sum(wp."American Indian- and Alaska Native-owned firms") over(partition by wp.state_abbreviation) "American Indian- and Alaska Native-owned firms",
sum(wp."Asian-owned firms") over(partition by wp.state_abbreviation) "Asian-owned firms",
sum(wp."Native Hawaiian- and Other Pacific Islander-owned firms") over(partition by wp.state_abbreviation) "Native Hawaiian- and Other Pacific Islander-owned firms",
sum(wp."Hispanic-owned firms") over(partition by wp.state_abbreviation) "Hispanic-owned firms",
sum(wp."Women-owned firms") over(partition by wp.state_abbreviation) "Women-owned firms"
from weighted_percent wp
order by wp.state_abbreviation)