1.
merge (n:Rest {rid:"70000"})
set n.name = "Taco Jacks",
	n.loc_stree = "Carretera Central Sn",
    n.parkingArrange = "none"

match (n:Rest {rid :"70000"}), (c:City {city:"slp"})
merge (n) -[:locate_in]-> (c)

match (n:Rest {rid :"70000"}), (f1:Feature {feature:"No_Alcohol_Served"})
merge (n) -[:has_feature]-> (f1)

match (n:Rest {rid :"70000"}), (f1:Feature {feature:"not permitted smoking"})
merge (n) -[:has_feature]-> (f1)

match (n:Rest {rid :"70000"}), (f1:Feature {feature:"informal dress"})
merge (n) -[:has_feature]-> (f1)

match (n:Rest {rid :"70000"}), (f1:Feature {feature:"completely access"})
merge (n) -[:has_feature]-> (f1)

match (n:Rest {rid :"70000"}), (f1:Feature {feature:"medium price"})
merge (n) -[:has_feature]-> (f1)

match (n:Rest {rid :"70000"}),(f1:Feature {feature:"t franchise"})
merge (n) -[:has_feature]-> (f1)

match (n:Rest {rid :"70000"}), (f1:Feature {feature:"open area"})
merge (n) -[:has_feature]-> (f1)

match (n:Rest {rid :"70000"}), (f1:Feature {feature:"Internet otherService"})
merge (n) -[:has_feature]-> (f1)

match (n:Rest {rid :"70000"}),(f1:Payment)
merge (n) -[:accept_payment]-> (f1)

match (n:Rest {rid :"70000"}), (f1:Cuisines {cuisine:"mexican"})
merge (n) -[:has_cuisine]-> (f1)

match (n:Rest {rid :"70000"}), (f1:Cuisines {cuisine:"burgers"})
merge (n) -[:has_cuisine]-> (f1)

match (n:Rest {rid :"70000"}), (f1:Day)
merge (n) -[r:openIn {openingHours:"9:00-20:00"}]- (f1);

match (:Day {day:"Sun"}) -[r1]- (n:Rest {rid :"70000"}) -[r]- (f1:Day {day:"Sat"})
set r.openingHours = "12:00-18:00",
	r1.openingHours = "12:00-18:00"



2.
match (n:User {uid:"1108"}) -[r]- (u: Cuisines {cuisine:"fast_food"})
delete r

match (n:User {uid:"1108"}) -[r]- (u: Payment {payment:"cash"})
delete r;
merge(n:User {uid:"1108"}) -[r:preferPay]- (u: Payment {payment:"debit cards"})

3.
match (n:User{uid:"1063"}) 
detach delete n