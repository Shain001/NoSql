Load userProfile.csv

LOAD CSV WITH HEADERS FROM "file:///userProfile.csv" AS row 
WITH row WHERE row._id IS NOT NULL 
MERGE (u : User {user_id : row._id})
set u.uid = row._id,
	u.loc_lat = coalesce(toInteger(row.`location latitude`),"Unkown"),
	u.loc_long = coalesce(toInteger(row.`location lontitude`),"Unkown"),
	u.birth_year = coalesce(date(row.`personalTraits birthYear`),"Unkown"), 
	u.weight = coalesce((row.`personalTraits weight`),"Unkown"),
	u.height = coalesce((row.`personalTraits height`),"Unkown"),
	u.marrital = coalesce(toLower(row.`personalTraits maritalStatus`),"Unkown"),
	u.fav_color = coalesce(toLower(row.`personality favColor`),"Unkown"),
	u.budget = coalesce(toLower(row.`preferences budget`),"Unkown"), 
	u.smoke = coalesce(toLower(row.`preferences smoker`),"Unkown"),
	u.dress = coalesce(toLower(row.`preferences dressPreference`),"Unkown"),
	u.ambience = coalesce(toLower(row.`preferences ambience`),"Unkown"),
	u.religion = coalesce(toLower(row.`otherDemographics religion`),"Unkown")

# load in cuisine entities
LOAD CSV WITH HEADERS FROM "file:///userProfile.csv" AS row 
with row where row._id is not null
Unwind split(row.favCuisines,", ") AS cuisines
Merge (c : Cuisines {cuisine:coalesce(toLower(cuisines),"Unknown")})

# load in payment entities
LOAD CSV WITH HEADERS FROM "file:///userProfile.csv" AS row 
with row where row._id is not null
Unwind split(row.favPaymentMethod,", ") AS pay
merge (p : Payment {payment : coalesce(toLower(pay),"Unknown")})

#load in worker type entities
LOAD CSV WITH HEADERS FROM "file:///userProfile.csv" AS row 
with row where row._id is not null
MERGE (w : TypeOfWorking {type : coalesce(toLower(row.`personality typeOfWorker`),"Unknown")})

--------------------
#load in transport entitiese
LOAD CSV WITH HEADERS FROM "file:///userProfile.csv" AS row 
with row where row._id is not null
MERGE (t : Transport {transport:coalesce(toLower(row.`preferences transport`),"Unknown")})

LOAD CSV WITH HEADERS FROM "file:///userProfile.csv" AS row 
with row where row._id is not null
MERGE (e : Employement {employment : coalesce(toLower(row.`otherDemographics employment`),"Unknown")})

-------

# create relationship in userProfile.csv
#1. user & type of work
LOAD CSV WITH HEADERS FROM "file:///userProfile.csv" AS row
with row where row.`personality typeOfWorker` is not null
match (u1 :User {user_id : row._id}),(w1 : TypeOfWorking {type : toLower(row.`personality typeOfWorker`)})
merge (u1) -[r1:Work_As]-> (w1)
with row where row.`personality typeOfWorker` is null
match (u2 :User {user_id : row._id}),(w2 : TypeOfWorking {type : "Unknown"})
merge (u2) -[r2:Work_As]-> (w2)

#2. user & transport

LOAD CSV WITH HEADERS FROM "file:///userProfile.csv" AS row
with row where row._id is not null
match (u1 :User {user_id : row._id})
with u1, row
match(t1 : Transport)
where t1.transport = coalesce(toLower(row.`preferences transport`),"Unknown")
merge (u1) -[r1:Prefer_Trans]-> (t1)

#3. user & cuisine
LOAD CSV WITH HEADERS FROM "file:///userProfile.csv" AS row
with row where row._id is not null
match (u1 :User {user_id : row._id})
unwind split(row.favCuisines, ", ") as fav
with u1, fav
match(c1 : Cuisines)
where c1.cuisine = coalesce(toLower(fav),"Unknown")
merge (u1) -[r1:Likes]-> (c1)

#4. user & payment
LOAD CSV WITH HEADERS FROM "file:///userProfile.csv" AS row
with row where row._id is not null
match (u :User {user_id : row._id})
with u, row
Unwind split(row.favPaymentMethod,", ") AS pay
match (p :Payment)
where p.payment = coalesce(toLower(pay),"Unkown")
merge (u) -[r:Prefer_Pay]-> (p)
return u,r,p

# user & employment
LOAD CSV WITH HEADERS FROM "file:///userProfile.csv" AS row
with row where row._id is not null
match (u :User {user_id : row._id})
with u, row
match (e :Employement)
where e.employment = coalesce(toLower(row.`otherDemographics employment`),"Unknown")
merge (u) -[r:Employ]-> (e)
return u,r,e



	Read place.csv

LOAD CSV WITH HEADERS FROM "file:///places.csv" AS row 
WITH row WHERE row._id IS NOT NULL 
MERGE (p : Rest {rid : row._id})
set p.rid = row._id,
	p.name = coalesce(toLower(row.placeName),"Unkown"),
	p.loc_lat = coalesce(toInteger(row.`location latitude`),"Unkown"),
	p.loc_long = coalesce(toInteger(row.`location longitude`),"Unkown"),
    p.parkingArrange = toLower(row.parkingArragements),
	p.loc_street = CASE row.`address street` WHEN "?" THEN "Unknown" ELSE toLower(row.`address street`)END
	



# city entity

------
\\ import city
LOAD CSV WITH HEADERS FROM "file:///places.csv" AS row 
WITH row WHERE row._id IS NOT NULL
with row,(case trim(row.`address city`) when "?" then "Unknown" else toLower(trim(row.`address city`)) END) as city
MERGE (c : City {city : city})

\\import state
LOAD CSV WITH HEADERS FROM "file:///places.csv" AS row 
WITH row WHERE row._id IS NOT NULL
with (case trim(row.`address state`) when "?" then "Unknown" else toLower(trim(row.`address state`)) END) as state
MERGE (s : State {state : state})

\\import country
LOAD CSV WITH HEADERS FROM "file:///places.csv" AS row 
WITH row WHERE row._id IS NOT NULL
with (case trim(row.`address country`) when "?" then "Unknown" else trim(row.`address country`) END) as country
MERGE (c2 : Country {country : country})

\\place & city
LOAD CSV WITH HEADERS FROM "file:///places.csv" AS row 
WITH row WHERE row._id IS NOT NULL
with row,(case trim(row.`address city`) when "?" then "Unknown" else toLower(trim(row.`address city`)) END) as city1
match (r:Rest {rid:row._id}),(c:City {city:city1})
merge (r) -[:Locate_In]-> (c)

\\city& state
LOAD CSV WITH HEADERS FROM "file:///places.csv" AS row 
WITH row WHERE row._id IS NOT NULL
with row,(case trim(row.`address city`) when "?" then "Unknown" else toLower(trim(row.`address city`)) END) as city1,(case trim(row.`address state`) when "?" then "Unknown" else toLower(trim(row.`address state`)) END) as state1
match (s:State {state:state1}),(c:City {city:city1})
merge (s) <-[:Of_State]- (c)

\\state&country
LOAD CSV WITH HEADERS FROM "file:///places.csv" AS row 
WITH row WHERE row._id IS NOT NULL
with row,(case trim(row.`address country`) when "?" then "Unknown" else trim(row.`address country`) END) as country1,(case trim(row.`address state`) when "?" then "Unknown" else toLower(trim(row.`address state`)) END) as state1
match (s:State {state:state1}),(c:Country {country:country1})
merge (s) -[:Of_Country]-> (c)


-------

# feature entity

LOAD CSV WITH HEADERS FROM "file:///places.csv" AS row 
WITH row WHERE row._id IS NOT NULL 
merge (f1 : Feature {feature : row.`placeFeatures smoking_area` + " smoking"})
merge (f2 : Feature {feature : row.`placeFeatures alcohol`})
merge (f3 : Feature {feature : row.`placeFeatures dress_code` + " dress"})
merge (f4 : Feature {feature : row.`placeFeatures accessibility` + " access"})
merge (f5 : Feature {feature : row.`placeFeatures price` + " price"})
merge (f6 : Feature {feature : row.`placeFeatures franchise` + " franchise"})
merge (f7 : Feature {feature : row.`placeFeatures area` + " area"})
merge (f8 : Feature {feature : row.`placeFeatures otherServices` + " otherService"})


# place & feature relationship

LOAD CSV WITH HEADERS FROM "file:///places.csv" AS row
with row where row._id is not null
match (r :Rest {rid : row._id})
with r, row
match (f :Feature)
where f.feature = row.`placeFeatures smoking_area` + " smoking"
merge (r) -[r2:Has_Feature]-> (f)
return r,r2,f

LOAD CSV WITH HEADERS FROM "file:///places.csv" AS row
with row where row._id is not null
match (r :Rest {rid : row._id})
with r, row
match (f :Feature)
where f.feature = row.`placeFeatures alcohol`
merge (r) -[r2:Has_Feature]-> (f)
return r,r2,f

LOAD CSV WITH HEADERS FROM "file:///places.csv" AS row
with row where row._id is not null
match (r :Rest {rid : row._id})
with r, row
match (f :Feature)
where f.feature = row.`placeFeatures dress_code`+ " dress"
merge (r) -[r2:Has_Feature]-> (f)
return r,r2,f

LOAD CSV WITH HEADERS FROM "file:///places.csv" AS row
with row where row._id is not null
match (r :Rest {rid : row._id})
with r, row
match (f :Feature)
where f.feature = row.`placeFeatures accessibility`+ " access"
merge (r) -[r2:Has_Feature]-> (f)
return r,r2,f

LOAD CSV WITH HEADERS FROM "file:///places.csv" AS row
with row where row._id is not null
match (r :Rest {rid : row._id})
with r, row
match (f :Feature)
where f.feature = row.`placeFeatures price`+ " price"
merge (r) -[r2:Has_Feature]-> (f)
return r,r2,f

LOAD CSV WITH HEADERS FROM "file:///places.csv" AS row
with row where row._id is not null
match (r :Rest {rid : row._id})
with r, row
match (f :Feature)
where f.feature = row.`placeFeatures franchise`+ " franchise"
merge (r) -[r2:Has_Feature]-> (f)
return r,r2,f

LOAD CSV WITH HEADERS FROM "file:///places.csv" AS row
with row where row._id is not null
match (r :Rest {rid : row._id})
with r, row
match (f :Feature)
where f.feature = row.`placeFeatures area` + " area"
merge (r) -[r2:Has_Feature]-> (f)
return r,r2,f

LOAD CSV WITH HEADERS FROM "file:///places.csv" AS row
with row where row._id is not null
match (r :Rest {rid : row._id})
with r, row
match (f :Feature)
where f.feature = row.`placeFeatures otherServices`+ " otherService"
merge (r) -[r2:Has_Feature]-> (f)
return r,r2,f

# place & payment relationship
LOAD CSV WITH HEADERS FROM "file:///places.csv" AS row
with row where row._id is not null and row.acceptedPaymentModes <> "any"
match (r :Rest {rid : row._id})
with r, row
Unwind split(row.acceptedPaymentModes,", ") AS pay
match (p :Payment)
where p.payment = coalesce(toLower(pay),"Unkown")
merge (r) -[:Accept_Payment]-> (p)

LOAD CSV WITH HEADERS FROM "file:///places.csv" AS row
with row where row._id is not null and row.acceptedPaymentModes = "any"
match (r :Rest {rid : row._id}),(p:Payment)
merge (r) -[:Accept_Payment]-> (p)


# place & cuisine
LOAD CSV WITH HEADERS FROM "file:///places.csv" AS row
with row where row._id is not null
match (r :Rest {rid : row._id})
with r, row
Unwind split(row.cuisines,", ") AS cuisines
match (c :Cuisines)
where c.cuisine = toLower(cuisines)
merge (r) -[r2:Has_Cuisine]-> (c)
return r,r2,c

	read opening_hours
	// 有陌生饭店
LOAD CSV WITH HEADERS FROM "file:///openingHours.csv" AS row
with row where row.placeID is not null
unwind split(row.days,";") as day
merge (n:Rest {rid :row.placeID})
with n, day,row
merge (d:Day {day : day})
with n,day,d,row
merge(n) -[r:Open_On]-> (d)
on create
set r.openingDay = day,
	r.openingHour = row.hours


	read place ratins
LOAD CSV WITH HEADERS FROM "file:///place_ratings.csv" AS row
with row
match (u:User {uid:row.user_id})
with u,row
match (r:Rest {rid : row.place_id})
merge (u) -[r2:Rates]-> (r)
on create 
set r2.count = 1,
	r2.rating_food = [row.rating_food],
	r2.rating_place = [row.rating_place],
	r2.rating_service = [row.rating_service]
on match
set r2.count = r2.count + 1,
	r2.rating_food = r2.rating_food + row.rating_food,
	r2.rating_place = r2.rating_place + row.rating_place,
	r2.rating_service = r2.rating_service + row.rating_service

	













