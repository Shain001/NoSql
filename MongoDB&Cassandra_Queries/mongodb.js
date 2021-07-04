// C.1.1
use FIT5137A1MRDB

// C.1.2
db.createCollection("userProfiles")

db.createCollection("placeProfiles")

// C.1.4 b
db.createCollection("openingHours")

db.openingHours.aggregate(
    [{
            $group: {
                _id: "$placeID",
                openingHours: {
                    $addToSet: {
                        hours: "$hours",
                        days: "$days"
                    }
                }
            }
        },
        {
            $project: {
                _id: 1,
                openingHours: 1
            }
        },
        {
            $out: "openingHours"
        }
    ]
)

db.placeProfiles.aggregate([{
        $lookup: {
            from: "openingHours",
            localField: "_id",
            foreignField: "_id",
            as: "openInfo"
        }
    },
    {
        $unwind: {
            "path": "$openInfo",
            "preserveNullAndEmptyArrays": true
        }
    },
    {
        $project: {
            _id: 1,
            acceptedPaymentModes: 1,
            address: 1,
            cuisines: 1,
            location: 1,
            parkingArragements: 1,
            placeFeatures: 1,
            placeName: 1,
            openingHours: "$openInfo.openingHours"
        }
    },
    {
        $out: "placeProfiles"
    }
])

// C.2.1
db.placeProfiles.insertOne({
    "_id": "70000",
    "acceptedPaymentModes": "any",
    "address": {
        "city": "San Luis Potosi",
        "country": "Mexico",
        "state": "SLP",
        "street": "Carretera Central Sn"
    },
    "cuisines": "Mexican, Burgers",
    "parkingArragements": "none",
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
    "placeName": "Taco Jacks",
    "openingHours": [{
            "hours": "09:00-20:00;",
            "days": "Mon;Tue;Wed;Thu;Fri;"
        },
        {
            "hours": "12:00-18:00;",
            "days": "Sat;Sun;"
        }
    ]
})

// C.2.2
db.userProfiles.updateOne({
        _id: "1108"
    },
    [{
            $set: {
                favCuisines: {
                    $replaceOne: {
                        input: "$favCuisines",
                        find: "Fast_Food, ",
                        replacement: ""
                    }
                },
                favPaymentMethod: {
                    $replaceOne: {
                        input: "$favPaymentMethod",
                        find: "cash",
                        replacement: "debit_cards"
                    }
                }
            }
        },
        {
            $set: {
                favCuisines: {
                    $replaceOne: {
                        input: "$favCuisines",
                        find: "Fast_Food",
                        replacement: ""
                    }
                }
            }
        }
    ]
)

// C.2.3
db.userProfiles.deleteOne({
    _id: "1063"
})

// C.3.1
db.userProfiles.count()

// C.3.2
db.placeProfiles.count()

// C.3.7
db.userProfiles.find({
    "otherDemographics.employment": "student",
    "preferences.budget": "medium"
}, {
    _id: 1
}).pretty()

// C.3.7 OR with showing the full information:
db.userProfiles.find({
    "otherDemographics.employment": "student",
    "preferences.budget": "medium"
})

// C.3.8
db.userProfiles.aggregate([
    {
        $match: {
            favCuisines: {
                $regex: "Bakery"
            }
        }
    },
    {
        $project: {
            _id: 0,
            "user": "$_id"
        }
    },
    {
        $merge: {
            into: "Q8"
        }
    }
])

db.placeProfiles.aggregate([{
        $match: {
            cuisines: {
                $regex: "Bakery"
            }
        }
    },
    {
        $project: {
            cuisines: 1,
            "Restaruant": "$_id",
            _id: 0
        }
    },
    {
        $merge: {
            into: "Q8"
        }
    }
])

db.Q8.find({})

// C.3.9
db.placeProfiles.aggregate([{
        $match: {
            cuisines: "International",
            "openingHours.days": "Sun;",
            "openingHours.hours": {
                $nin: ["0:00-0:00;"]
            }
        }
    },
    {
        $project: {
            placeName: 1
        }
    }
])

// C.3.11
db.userProfiles.aggregate([{
        $project: {
            "birthYear": {
                $convert: {
                    input: "$personalTraits.birthYear",
                    to: "int"
                }
            },
            "drinkLevel": "$personality.drinkLevel"
        }
    }, {
        $project: {
            "age": {
                $subtract: [{
                    $year: new Date()
                }, "$birthYear"]
            },
            "drinkLevel": 1
        }
    },
    {
        $group: {
            _id: "$drinkLevel",
            avgAge: {
                $avg: "$age"
            }
        }
    }
])

// C.3.13
db.userProfiles.aggregate([{
        $match: {
            favCuisines: {
                $regex: "Japanese"
            },
            "personalTraits.maritalStatus": "single"
        }
    },
    {
        $project: {
            favCuisines: 1,
            "marital": "$personalTraits.maritalStatus",
            "ambience": "$preferences.ambience"
        }
    },
    {
        $group: {
            _id: "$ambience",
            count: {
                $sum: 1
            }
        }
    },
    {
        $sort: {
            count: -1
        }
    },
    {
        $limit: 3
    }
])

// C.3.14
db.placeProfiles.aggregate([{
        $project: {
            cuisines: {
                $split: ["$cuisines", ","]
            },
            _id: 1
        }
    },
    {
        $unwind: "$cuisines"
    },
    {
        $group: {
            _id: null,
            uniqueCuisines: {
                $addToSet: "$cuisines"
            }
        }
    },
    {
        $project: {
            uniqueCuisines: 1
        }
    }
])

// C.3.15
db.placeProfiles.aggregate({
    $project: {
        placeName: 1,
        cuisines: 1,
        Serving: {
            $cond: {
                if: {
                    $in: ["$cuisines", ["Mexico"]]
                },
                then: "Mexican Served",
                else: "Mexican Not Served"
            }
        }
    }
})

// C.3 Additional Query 2
db.placeProfiles.aggregate([{
        $project: {
            parkingArragements: 1,
        }
    },
    {
        $group: {
            _id: "$parkingArragements",
            count: {
                $sum: 1
            }
        }
    },
    {
        $project: {
            count: 1,
            parkingArragements: 1,
            percentage: {
                $concat: [{
                    $substr: [{
                        $multiply: [{
                            $divide: ["$count", {
                                "$literal": db.placeProfiles.count()
                            }]
                        }, 100]
                    }, 0, 5]
                }, "%"]
            }
        }
    }
])

// C.3 Additional Query 3
db.userProfiles.aggregate([{
        $project: {
            "favPaymentMethod": {
                $split: ["$favPaymentMethod", ","]
            }
        }
    },
    {
        $unwind: "$favPaymentMethod"
    },
    {
        $group: {
            _id: "$favPaymentMethod",
            count: {
                $sum: 1
            }
        }
    },
    {
        $sort: {
            "count": -1
        }
    },
    {
        $project: {
            favPaymentMethod: 1,
            count: 1,
            percentage: {
                $concat: [{
                    $substr: [{
                        $multiply: [{
                            $divide: ["$count", {
                                "$literal": db.userProfiles.count()
                            }]
                        }, 100]
                    }, 0, 5]
                }, "%"]
            }
        }
    }
])

// C.3 Additional Query 4
db.userProfiles.aggregate([{
        $project: {
            employement: "$otherDemographics.employment",
            budget: "$preferences.budget"
        }
    },
    {
        $group: {
            _id: {
                employement: "$employement",
                budget: "$budget"
            },
            numberOfUers: {
                $sum: 1
            }
        }
    },
    {
        $sort: {
            numberOfUers: -1
        }
    }
])

// C.3 Additional Query 5
db.userProfiles.aggregate([{
        $project: {
            "personality.drinkLevel": 1
        }
    },
    {
        $group: {
            _id: "$personality.drinkLevel",
            count: {
                $sum: 1
            }
        }
    },
    {
        $sort: {
            "count": -1
        }
    }
])

// two index
db.placeProfiles.createIndex({
    cuisines: 1,
    parkingArragements: 1
})

db.userProfiles.createIndex({
    "personality.drinkLevel": 1,
    "otherDemographics.employment": 1,
    "favCuisines": 1,
    "preferences.budget": 1
})




