
CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.dwh_dataset }}.fact_fixture` AS
select f.fixture_id,
       f.fixture_date,
       h.team_name                                         home_team,
       a.team_name                                         away_team,
       concat(v.address,', ',v.city)                       address
  from `{{ params.project_id }}.{{ params.staging_dataset }}.fixture` f
 inner join `{{ params.project_id }}.{{ params.staging_dataset }}.team` h
    on f.teams_home_id = h.team_id
 inner join `{{ params.project_id }}.{{ params.staging_dataset }}.team` a
    on f.teams_away_id = a.team_id
 inner join `{{ params.project_id }}.{{ params.staging_dataset }}.venue` v
    on f.fixture_venue_id = v.id
;
