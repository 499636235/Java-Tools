select *
from (select * from soochow_data.ods_lis_lccont )a 
	inner join soochow_data.ods_lis_lcpol b
		on a.contno = b.contno
where a.contno = '0000000000'
and b.polno like '1%'