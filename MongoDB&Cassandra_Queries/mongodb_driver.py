#!/usr/bin/env python3

import pymongo
import json

client = pymongo.MongoClient('localhost', 27017)
# C.1.1
# create a data database
db = client['FIT5137A1MRDB']
# C.1.2
# create 2 collections
placeProfiles = db['placeProfiles']
userProfiles = db['userProfiles']
# C.1.3 a
with open('userProfile.json') as user_json:
    user_data = json.load(user_json)
insert_result = userProfiles.insert_many(user_data)
insert_result.acknowledged
# C.1.3 b
with open('placeProfiles.json') as place_json:
    place_data = json.load(place_json)
insert_result = placeProfiles.insert_many(place_data)
insert_result.acknowledged
# C.1.4 b
openingHours = db['openingHours']


def format_data(path):
    arr = []
    with open(path, "r") as files:
        next(files)
        for file in files:
            file = file.replace('\n', '')
            columns = file.split(",")
            obj = {
                "placeID": columns[0],
                "hours": columns[1],
                "days": columns[2]
            }
            arr.append(obj)


#     print(arr)
    return arr


def find(find_result):
    for result in find_result:
        print(result)


insert_result = openingHours.insert_many(format_data("openingHours.csv"))
insert_result.acknowledged

openingHours.aggregate([{
    "$group": {
        "_id": "$placeID",
        "openingHours": {
            "$addToSet": {
                "hours": "$hours",
                "days": "$days"
            }
        }
    }
}, {
    "$project": {
        "_id": 1,
        "openingHours": 1
    }
}, {
    "$out": "openingHours"
}])

placeProfiles.aggregate([{
    "$lookup": {
        "from": "openingHours",
        "localField": "_id",
        "foreignField": "_id",
        "as": "openInfo"
    }
}, {
    "$unwind": {
        "path": "$openInfo",
        "preserveNullAndEmptyArrays": True
    }
}, {
    "$project": {
        "_id": 1,
        "acceptedPaymentModes": 1,
        "address": 1,
        "cuisines": 1,
        "location": 1,
        "parkingArragements": 1,
        "placeFeatures": 1,
        "placeName": 1,
        "openingHours": "$openInfo.openingHours"
    }
}, {
    "$out": "placeProfiles"
}])

# C.2.1
placeProfiles.insert_one({
    "_id":
    "70000",
    "acceptedPaymentModes":
    "any",
    "address": {
        "city": "San Luis Potosi",
        "country": "Mexico",
        "state": "SLP",
        "street": "Carretera Central Sn"
    },
    "cuisines":
    "Mexican, Burgers",
    "parkingArragements":
    "none",
    "placeFeatures": {
        "accessibility": "completely",
        "alcohol": "No_Alcohol_Served",
        "area": "open",
        "dressCode": "informal",
        "franchise": "f",
        "otherServices": "Internet",
        "price": "medium",
        "smokingArea": "not permitted"
    },
    "placeName":
    "Taco Jacks",
    "openingHours": [{
        "hours": "09:00-20:00;",
        "days": "Mon;Tue;Wed;Thu;Fri;"
    }, {
        "hours": "12:00-18:00;",
        "days": "Sat;Sun;"
    }]
})

userProfiles.update_one({"_id": "1108"}, [{
    "$set": {
        "favCuisines": {
            "$replaceOne": {
                "input": "$favCuisines",
                "find": "Fast_Food, ",
                "replacement": ""
            }
        },
        "favPaymentMethod": {
            "$replaceOne": {
                "input": "$favPaymentMethod",
                "find": "cash",
                "replacement": "debit_cards"
            }
        }
    }
}, {
    "$set": {
        "favCuisines": {
            "$replaceOne": {
                "input": "$favCuisines",
                "find": "Fast_Food",
                "replacement": ""
            }
        }
    }
}])

userProfiles.delete_one({"_id": "1063"})

# C.3.1
print(
    f'How many users are there in the database? : {userProfiles.estimated_document_count()}'
)

# C.3.2
print(
    f'How many places are there in the database? : {placeProfiles.estimated_document_count()}'
)

# C.3.7
find_result = userProfiles.find(
    {
        "otherDemographics.employment": "student",
        "preferences.budget": "medium"
    }, {"_id": 1})
print("----C.3.7----")
find(find_result)

# C.3.8
Q8 = db['Q8']
userProfiles.aggregate([{
    "$match": {
        "favCuisines": {
            "$regex": "Bakery"
        }
    }
}, {
    "$project": {
        "_id": 0,
        "user": "$_id"
    }
}, {
    "$merge": {
        "into": "Q8"
    }
}])

placeProfiles.aggregate([{
    "$match": {
        "cuisines": {
            "$regex": "Bakery"
        }
    }
}, {
    "$project": {
        "cuisines": 1,
        "Restaruant": "$_id",
        "_id": 0
    }
}, {
    "$merge": {
        "into": "Q8"
    }
}])

find_result = Q8.find({})
print("----C.3.8----")
find(find_result)

# C.3.9
find_result = placeProfiles.aggregate([{
    "$match": {
        "cuisines": "International",
        "openingHours.days": "Sun;",
        "openingHours.hours": {
            "$nin": ["0:00-0:00;"]
        }
    }
}, {
    "$project": {
        "placeName": 1
    }
}])
print("----C.3.9----")
find(find_result)

# C.3.11
from datetime import datetime

find_result = userProfiles.aggregate([{
    "$project": {
        "birthYear": {
            "$convert": {
                "input": "$personalTraits.birthYear",
                "to": "int"
            }
        },
        "drinkLevel": "$personality.drinkLevel"
    }
}, {
    "$project": {
        "age": {
            "$subtract": [{
                "$year": datetime.now()
            }, "$birthYear"]
        },
        "drinkLevel": 1
    }
}, {
    "$group": {
        "_id": "$drinkLevel",
        "avgAge": {
            "$avg": "$age"
        }
    }
}])
print("----C.3.11----")
find(find_result)

# C.3.14
find_result = placeProfiles.aggregate([{
    "$project": {
        "cuisines": {
            "$split": ["$cuisines", ","]
        },
        "_id": 1
    }
}, {
    "$unwind": "$cuisines"
}, {
    "$group": {
        "_id": None,
        "uniqueCuisines": {
            "$addToSet": "$cuisines"
        }
    }
}, {
    "$project": {
        "uniqueCuisines": 1
    }
}])
print("----C.3.14----")
find(find_result)

# C.3.15
find_result = placeProfiles.aggregate([
 {
  "$project":{
   "placeName":1,
   "cuisines":1,
   "Serving":{
    "$cond":{
     "if":{
      "$in":["$cuisines",["Mexico"]]
     },
     "then":"Mexican Served",
     "else":"Mexican Not Served"
    }
   }
  }
 }
])
print("----C.3.15----")
find(find_result)

# Additional Query 2
find_result = placeProfiles.aggregate([
 {
  "$project":{
   "parkingArragements":1,
  }
 },
 {
  "$group":{
   "_id":"$parkingArragements",
   "count":{
    "$sum":1
   }
  }
 },
 {
  "$project":{
   "count":1,
   "parkingArragements":1,
   "percentage":{
    "$concat":[
    {
     "$substr":
     [{
     "$multiply":[
    {
     "$divide":["$count",{"$literal": placeProfiles.estimated_document_count()}]
    },100]
    },0,5]
    },"%"
    ]
   }
   }
 }
])
print("----Additional Query 2----")
find(find_result)

# Additional Query 3
find_result = userProfiles.aggregate([{
    "$project": {
        "favPaymentMethod": {
            "$split": ["$favPaymentMethod", ","]
        }
    }
}, {
    "$unwind": "$favPaymentMethod"
}, {
    "$group": {
        "_id": "$favPaymentMethod",
        "count": {
            "$sum": 1
        }
    }
}, {
    "$sort": {
        "count": -1
    }
}, {
    "$project": {
        "favPaymentMethod": 1,
        "count": 1,
        "percentage": {
            "$concat": [{
                "$substr": [{
                    "$multiply": [{
                        "$divide": [
                            "$count", {
                                "$literal":
                                userProfiles.estimated_document_count()
                            }
                        ]
                    }, 100]
                }, 0, 5]
            }, "%"]
        }
    }
}])
print("----Additional Query 3----")
find(find_result)

# Additional Query 4
find_result = userProfiles.aggregate([{
    "$project": {
        "employement": "$otherDemographics.employment",
        "budget": "$preferences.budget"
    }
}, {
    "$group": {
        "_id": {
            "employement": "$employement",
            "budget": "$budget"
        },
        "numberOfUers": {
            "$sum": 1
        }
    }
}, {
    "$sort": {
        "numberOfUers": -1
    }
}])
print("----Additional Query 4----")
find(find_result)

# Additional Query 5
find_result = userProfiles.aggregate([{
    "$project": {
        "personality.drinkLevel": 1
    }
}, {
    "$group": {
        "_id": "$personality.drinkLevel",
        "count": {
            "$sum": 1
        }
    }
}, {
    "$sort": {
        "count": -1
    }
}])
print("----Additional Query 5----")
find(find_result)
