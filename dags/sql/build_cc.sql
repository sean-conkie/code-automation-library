select * except(file_dt, file_name, load_dt {{params.EXCLUDE}})
from (select *,
			row_number() over (partition by {{params.KEY}}
								   order by file_dt desc, load_dt desc) rnum
		from {{params.PROJECT_NAME}}.{{params.DATASET_ID}}.{{params.FROM}})
where rnum = 1
;