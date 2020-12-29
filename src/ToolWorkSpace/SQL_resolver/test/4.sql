select max(a.id)
from (Select* From soochow_data.ods_lis_lccont a1
				join (select * from soochow_data.ods_lis_lccont2) a2
					on a1.id = a2.id )a 
	inner join soochow_data.ods_lis_lcpol b
		on a.contno = b.contno--sdfsaf
where a.contno = '0000000000'
and b.polno like '1%'