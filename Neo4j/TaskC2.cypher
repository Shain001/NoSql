create index Rest for (r:Rest) on (r.rid,r.name);
create index Rates for (r:Rates) on (r.rating_food,r.rating_service,r.rating_place);
create index City for (c:City) on (c.city);

1. 
match (r : Rest {name: "chilis cuernavaca"}) <-[r2]- () return sum(r2.count)

2.
match (c:City) -[:Of_State]-> (s:State {state:"morelos"})
with c,s
match (r:Rest) -[:Locate_In]-> (c)
with r,s
match (r) -[:Has_Cuisine]-> (cs:Cuisines)
with r, cs,s
match (r) <-[rt:Rates]- (u:User)
return s.state as state, r.rid as place_id,r.name as place_name, collect(distinct cs.cuisine) as cuisines, collect(rt.rating_service) as rating_service


3.
match (u1:User {uid:"1033"}) -[rt:Rates]-> (r:Rest)
where toInteger(rt.rating_food[0]) > 1 and toInteger(rt.rating_place[0]) > 1 and toInteger(rt.rating_service[0]) > 1
with r
match (u2:User {uid:"1003"})
where not (u2) -[:Rates]-> (r)
return r




4.
match (r:Rest) 
where not (r) -[:Has_Cuisine]-> (:Cuisines {cuisine:"mexican"})
match (c:City)
with r,c
match (r) -- (c) 
return {place_name:r.name, place_city:c.city, place_lat:r.loc_lat, place_long:r.loc_long} 

5.
match (u:User) -[rt:Rates]-> (r:Rest)
return u.uid, sum(rt.count)


// or  --- since there is no customers rates a same restaruant more than one times
match (u:User) -[rt:Rates]-> (r:Rest)
return u.uid, count(rt)


6.
match (r1: Rest),(r2: Rest)
where size((r1) -[:Has_Feature]-> (:Feature) <-[:Has_Feature]- (r2)) >3
return{r1:r1.rid,r2:r2.rid}

7.
match (c:Cuisines {cuisine:"international"})
with c
match (n:Rest) -- (c)
with n
match(d:Day {day:"Sun"}) <-[:Open_On]- (n)
return n


8.
match (c :City)
where c.city =~".*victoria.*"
with c
match (r :Rest) -- (c)
with r
match (r) -[rt:Rates]- (:User)
return avg(rt.rating_food[0])


9.
match (n:City) -- (r:Rest) -[rt:Rates]- (u:User)
with n, avg(rt.rating_service[0]) as rates
order by rates desc 
limit 3
return n, rates 


10.
match (n:Rest),(n2:Rest)
where n.rid<>n2.rid
with n.name as name1,n2, distance(point({longitude:n.loc_long,latitude:n.loc_lat}), point({ longitude: n2.loc_long, latitude: n2.loc_lat })) as dis
order by dis desc
return {place:name1, ranks: collect(n2.name) } 









