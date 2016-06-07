select
	com.duedil as duedil,
	com.name as company_name,
	com.linkedin_id,
	com.industry_id,
	ind.name as industry_name,
	com.sic2007code,
	ST_Y( com.point ) as latitude, 
	ST_X( com.point) as longitude,
	com.other_names,
	com.turnover, com.previous_turnover, com.turnover_growth,
	com.employees, com.previous_employees, com.employee_growth,
	gwc."name" as constituency,
	gwc.population as constituency_population,
	gwc.mp as company_hq_mp,
	gwc.party as company_hq_political_party,
	com.updated as company_updated
	
 from leaders_company com, leaders_industry ind, 
 governments_westminsterconstituency gwc,
 governments_ward gwa
 
where com.industry_id = ind.id
and com.ward_id = gwa.id
and gwa.constituency_id = gwc.id
