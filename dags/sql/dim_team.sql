select league.name league,
       team.season,
       team.team_name,
       team.founded,
       team.stadium,
       team.address,
       team.capacity
  from (select params.season,
               team.name team_name,
               team.founded,
               venue.name stadium,
               concat(venue.address,', ', venue.city) address,
               venue.capacity,
               params.league
          from uk_tds_football_is.cc_team t,
               unnest(response) resp,
               unnest(resp.team) team,
               unnest(resp.venue) venue,
               unnest(parameters) params) team
  left join (select l.name,
                    l.id,
                    params.season
               from uk_tds_football_is.cc_league cc,
                    unnest(parameters) params,
                    unnest(league) l) league
    on (    league.id     = team.league
        and league.season = team.season)
;
