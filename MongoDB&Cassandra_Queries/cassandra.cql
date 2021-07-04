-- C.1.5
create keyspace FIT5137A1_MRDB with
    replication = {
        'class': 'SimpleStrategy',
        'replication_factor':1
    };
use FIT5137A1_MRDB;

-- C.1.6 a
CREATE TYPE user_personal_traits_type (
   birth_year INT,
   weight INT,
   height FLOAT,
   marital_status TEXT
);
CREATE TYPE user_personality_type (
   interest TEXT,
   type_of_worker TEXT,
   fav_color TEXT,
   drink_level TEXT
);
CREATE TYPE user_preferences_type (
   budget TEXT,
   smoker BOOLEAN,
   dress_preference TEXT,
   ambience TEXT,
   transport TEXT
);
CREATE TYPE user_other_demographics_type (
   religion TEXT,
   employment TEXT
);
CREATE TABLE user_ratings (
   rating_id INT,
   user_id TEXT,
   place_id TEXT,
   rating_place INT,
   rating_food INT,
   rating_service INT,
   user_personal_traits FROZEN<user_personal_traits_type>,
   user_personality FROZEN<user_personality_type>,
   user_preferences FROZEN<user_preferences_type>,
   user_other_demographics FROZEN<user_other_demographics_type>,
   user_fav_cuisines SET<TEXT>,
   user_fav_payment_method SET<TEXT>,
   PRIMARY KEY (rating_id, user_id)
);

-- C.1.6 b
CREATE TYPE place_address_type (
   street TEXT,
   city TEXT,
   state TEXT,
   country TEXT
);
CREATE TYPE place_features_type (
   alcohol TEXT,
   smoking_area TEXT,
   dress_code TEXT,
   accessibility TEXT,
   price TEXT,
   franchise TEXT,
   area TEXT,
   other_services TEXT
);
CREATE TABLE place_ratings (
   rating_id INT,
   user_id TEXT,
   place_id TEXT,
   rating_place INT,
   rating_food INT,
   rating_service INT,
   place_name TEXT,
   place_address FROZEN<place_address_type>,
   place_features FROZEN<place_features_type>,
   parking_arrangements TEXT,
   accepted_payment_modes SET<TEXT>,
   cuisines FROZEN<SET<TEXT>>,
   PRIMARY KEY (rating_id, user_id)
);

-- C.1.7 a
COPY FIT5137A1_MRDB.user_ratings (rating_id, user_id, place_id, rating_place, rating_food, rating_service, user_personal_traits, user_personality, user_preferences, user_other_demographics, user_fav_cuisines, user_fav_payment_method) FROM 'user_ratings.csv' WITH HEADER = true;
-- C.1.7 b
COPY FIT5137A1_MRDB.place_ratings (rating_id, user_id, place_id, rating_place, rating_food, rating_service, place_name, place_address, place_features, parking_arrangements, accepted_payment_modes, cuisines) FROM 'place_ratings.csv' WITH HEADER = true;

-- C.2.4
UPDATE user_ratings
SET user_fav_cuisines = user_fav_cuisines - {'Fast_Food'}
WHERE rating_id IN (65, 66, 67, 68, 69, 70, 71, 72, 73, 74) AND user_id = '1108';

UPDATE user_ratings
SET user_fav_payment_method = user_fav_payment_method - {'cash'}, user_fav_payment_method = user_fav_payment_method + {'debit_cards'}
WHERE rating_id IN (65, 66, 67, 68, 69, 70, 71, 72, 73, 74) AND user_id = '1108';

DELETE FROM user_ratings WHERE rating_id IN (137, 138, 139, 140, 141) AND user_id = '1063';

-- C.2.5
INSERT INTO user_ratings (
   rating_id,
   user_id,
   place_id,
   rating_place,
   rating_food,
   rating_service,
   user_personal_traits,
   user_personality,
   user_preferences,
   user_other_demographics,
   user_fav_cuisines,
   user_fav_payment_method
) VALUES (
   7777,
   '1060',
   '70000',
   2,
   1,
   2,
   {birth_year: 1991, weight: 82, height: 1.84, marital_status: 'single'},
   {interest: 'technology', type_of_worker: 'thrifty-protector', fav_color: 'blue', drink_level: 'casual drinker'},
   {budget: 'medium', smoker: False, dress_preference: 'formal', ambience: 'family', transport: 'public'},
   {religion: 'Catholic', employment: 'student'},
   {'American', 'Burgers', 'Cafe-Coffee_Shop', 'Cafeteria', 'Fast_Food', 'Hot_Dogs', 'Italian', 'Juice', 'Mexican', 'Pizzeria', 'Soup', 'Spanish', 'Tex-Mex'},
   {'cash'}
);

-- C.3.3
select count(*) from place_ratings;

-- C.3.4
CREATE INDEX parking ON place_ratings (parking_arrangements);
select count(*) from place_ratings where parking_arrangements = 'public';

-- C.3.5
CREATE INDEX user_persenality ON fit5137a1_mrdb.user_ratings(user_personality);
select user_id, rating_place,user_personality from user_ratings where user_personality = {interest: 'technology', type_of_worker: 'thrifty-protector', fav_color: 'blue', drink_level: 'casual drinker'};

-- C.3.6
create index cuisin_full on place_ratings(full(cuisines));
select place_id,rating_food,cuisines from place_ratings where cuisines = {'Pizzeria'};

-- C.3.10
create index place_names on place_ratings(place_name);
SELECT avg(cast(rating_place as float)) AS "place rating",avg(cast(rating_food as float)) AS "food rating",avg(cast(rating_service as float)) AS "service rating"  FROM place_ratings WHERE place_name = 'puesto de tacos';

-- C.3.12
create index fav_cuisine on user_ratings(user_fav_cuisines);
select user_id, place_id, rating_place, rating_food, user_preferences.budget from user_ratings where user_fav_cuisines contains 'Family';


-- C.3 Additional Query 1
select avg(cast(rating_place AS Double)) AS averagePlaceRating,parking_arrangements from place_ratings where parking_arrangements = 'none';