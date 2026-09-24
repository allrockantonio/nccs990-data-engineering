if object_id(N'dbo.Dim_Organization',N'U') is not null begin drop table dbo.Dim_Organization end;
if object_id(N'dbo.dim_state',N'U') is not null begin drop table dbo.dim_state end;

WITH org_ein_CTE AS (
	SELECT distinct F9_00_ORG_EIN as EIN,EIN2 FROM [raw].[F9-P00-T00-HEADER] 
),
org_info_cte as (
	select 
		F9_00_ORG_EIN EIN
		,upper(F9_00_ORG_NAME_L1) OrgName
		,upper(F9_00_ORG_NAME_L2) OrgName_L2
		,upper([F9_00_ORG_ADDR_L1]) Address
		,upper([F9_00_ORG_ADDR_CITY]) City 
		,upper([F9_00_ORG_ADDR_STATE]) State_Code
		,[F9_00_ORG_ADDR_ZIP] ZipCode		
		,Row_Number() over (partition by F9_00_ORG_EIN order by F9_00_TAX_YEAR desc) as RowNum
	from [raw].[F9-P00-T00-HEADER]
)

select distinct
	org_info_cte.EIN,EIN2,
	OrgName,OrgName_L2
	[Address],City,State_Code,ZipCode
	into Dim_Organization
from org_ein_CTE
left join org_info_cte on org_ein_CTE.EIN = org_info_cte.EIN and org_info_cte.RowNum = 1

select * into dim_state from
(
select distinct State_code statecode,State_code statename,State_Information='foreign' from dim_organization where State_code not in (select statecode from base_statecodes)
union all 
select statecode, StateName,State_Information='local' from base_statecodes
) ds


  
