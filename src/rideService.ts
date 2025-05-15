import cors from "cors";
import express from "express";
import { ApolloServer , gql } from "apollo-server-express";
import jwt from "jsonwebtoken";
import { DateTimeResolver , JSONResolver } from "graphql-scalars";
import * as dotenv from 'dotenv'
import { checkAuth , fetchRole , fetchId} from "./authorizer.ts";
import {PrismaClient } from "@prisma/client";
import {Kafka , Partitioners , logLevel} from "kafkajs";
import e from "cors";


dotenv.config();

(async function () { 
    let users : any[] = [];

    let cars : any[] = [];

    let ridesToday : any[] = [];


    const kafka = new Kafka({
        clientId: "RideService",
        brokers: [process.env.KAFKA_URL!],
    });

    const producer = kafka.producer({
        createPartitioner: Partitioners.LegacyPartitioner
    });
    const consumer = kafka.consumer({ groupId: "RideService"});

    // producer.logger().setLogLevel(logLevel.DEBUG);

    const shareRideDetails = async (ride : any) => {
        console.log("Entered Function");
        await producer.connect();
        const subsetRide = {id: ride.id , driverId : ride.driverId , basePrice: ride.basePrice , girlsOnly: ride.girlsOnly}
        await producer.send({
            topic: "ride-details",
            messages: [{
                key: ride.id.toString(),
                value: JSON.stringify(subsetRide)
            }]
        });
        console.log("Sent!");
        await producer.disconnect();
    };

    const notifyPassengers = async (rideId : any , rideTime : Date) => {
        const json = JSON.stringify({rideId : rideId , rideTime : rideTime});
        await producer.connect();
        await producer.send({
            topic: "notify-passengers",
            messages: [{
                key: rideId.toString(),
                value: json
            }]
        });
    };

    const notifyRideCompleted = async (rideId : any) => {
        await producer.connect();
        await producer.send({
            topic: "ride-completed",
            messages: [{
                key: rideId.toString(),
                value: JSON.stringify(rideId)
            }]
        });
    };

    const notifyRideCancelled = async (rideId : any) => {
        await producer.connect();
        await producer.send({
            topic: "ride-cancelled",
            messages: [{
                key: rideId.toString(),
                value: JSON.stringify(rideId)
            }]
        });
    };

    const shareSubzoneDetails = async (subzone : any) => {
        console.log("Entered Function");
        await producer.connect();
        const subsetSubzone = {subzoneName: subzone.subzoneName , areaName: subzone.areaName , subZonePrice: subzone.subZonePrice}
        await producer.send({
            topic: "subzone-details",
            messages: [{
                key: subzone.subzoneName.toString(),
                value: JSON.stringify(subsetSubzone)
            }]
        });
        console.log("Sent!");
        await producer.disconnect();
    };

    const periodicDateTimeCheck = async () => {
        const now : Date = new Date();
        let rides = await prisma.ride.findMany();
        const ridesTodayIds = rides.map(ride => ride.id);
        rides = rides.filter(rides => !ridesTodayIds.includes(rides.id));
        rides = rides.filter(ride => ride.active === true);
        rides.forEach(ride => {
            const differenceInYears = ride.time.getFullYear() - now.getFullYear();
            const differenceInMonths = ride.time.getMonth() - now.getMonth();
            const differenceInDays = ride.time.getDay() - now.getDay();
            if(differenceInYears === 0 && differenceInMonths === 0 && differenceInDays === 0)
            {
                notifyPassengers(ride.id , ride.time);
            }
            ridesToday.push(ride);
        })
    };

    const periodicTripCompletedCheck = async () => {
        const now : Date = new Date();
        let rides = await prisma.ride.findMany();
        rides = rides.filter(ride => ride.active === true);
        rides.forEach(async (ride) => {
            const differenceInYears = ride.time.getFullYear() - now.getFullYear();
            const differenceInMonths = ( ride.time.getMonth() + 1) - ( now.getDate() + 1);  
            const differenceInDays = ride.time.getDate() - now.getDate();
            const differenceInHours = ride.time.getHours() - now.getHours();
            if( (differenceInYears == 0 && differenceInMonths == 0 && differenceInDays < 0 ) || (differenceInYears == 0 && differenceInMonths == 0 && differenceInDays == 0 && differenceInHours <= -2))
            {
                notifyRideCompleted(ride.id);
                const completedRide = await prisma.ride.update({
                    where: {
                        id : ride.id
                    },
                    data : {
                        active : false
                    }
                });
            }
            ridesToday.filter(rideF => rideF.id !== ride.id);
        })
    };

    await consumer.connect();
    await consumer.subscribe({ topics: ["booking-share" , "seat-increase" , "seat-reduce" , "user-gender-details" , "car-info"]});

    await consumer.run({
        eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
            if(topic === "seat-increase")
            {
                console.log("Entered Seat Reduce");
                if(message.value !== null)
                {
                    const rideId = Number(message.value);
                    const ride = await prisma.ride.findUnique({
                        where: {
                            id : rideId
                        }
                    });
                    if(ride !== null)
                    {
                        const updatedRide = await prisma.ride.update({
                            where: {
                                id : rideId
                            },
                            data : {
                                seatsLeft : ride.seatsLeft + 1
                            }
                        });
                    }
                        
                }
            }
            else if(topic === "seat-reduce")
            {
                console.log("Entered Seat Reduce");
                if(message.value !== null)
                {
                    console.log("Message is: " + message.value);
                    const parsedMessage = JSON.parse(message.value.toString());
                    const rideId = Number(parsedMessage.rideId);
                    console.log("Ride ID is: " + rideId);
                    const ride = await prisma.ride.findUnique({
                        where: {
                            id : rideId
                        }
                    });
                    if(ride !== null)
                    {
                        const updatedRide = await prisma.ride.update({
                            where: {
                                id : rideId
                            },
                            data : {
                                seatsLeft : ride.seatsLeft - 1
                            }
                        });
                    }
                }
            }
            else if(topic === "user-gender-details")
            {
                if(message.value !== null)
                {
                    const user = JSON.parse(message.value.toString());
                        const userId = user.id.toString();
                        const gender = user.gender;
                        const newUser = await prisma.subUser.create({
                            data : {
                                id : Number(userId),
                                gender : gender
                            }
                        });
                }

            }
            else if(topic === "car-info")
            {
                if(message.value !== null)
                {
                    const car = JSON.parse(message.value.toString());
                    const carId = car.id;
                    const universityId = car.universityId;
                    const seats = car.seats;
                    const cars = await prisma.subCar.deleteMany({
                        where: {
                            universityId : universityId.toString()
                        }
                    });
                    const carInfo = await prisma.subCar.create({
                        data : {
                            id : carId,
                            universityId : universityId.toString(),
                            seats : seats
                        }
                    });
                }
            }
        },
    });

    const prisma = new PrismaClient();
    
    dotenv.config();

    const typeDefs = gql`
        scalar DateTime
        scalar Json

        type Ride {
            id: Int!
            driverId: Int!
            time: DateTime!
            areaName: String!
            basePrice: Float!
            seatsLeft: Int!
            active: Boolean!
            fromGiu: Boolean!
            girlsOnly: Boolean!
        }

        type Area {
            areaName: String!
            basePrice: Float!
            distanceFromGiu: Float!
            rides: [Ride]
            subzones: [Subzone]
        }

        type Subzone {
            subzoneName: String!
            areaName: String!
            subZonePrice: Float!
        }

        type SubCar {
            id: String!
            universityId: String!
            seats: Int!
        }

        type Query {
            addCookies(token: String!): String
            fetchAllAreas: [Area]
            fetchAllSubzones: [Subzone]
            fetchAllRides: [Ride]
            fetchRideByCriteria(areaName: String! , fromGiu: Boolean! , girlsOnly: Boolean!): [Ride]
            fetchMyRides: [Ride]
            fetchRide(id: Int!): Ride
            rideCompleted(id: Int!): Ride
        }

        type Mutation {
            createRide(time: DateTime! , areaName: String! fromGiu: Boolean! , girlsOnly: Boolean!): Ride
            
            updateRide(id: Int! , time: DateTime , areaName: String , seatsLeft: Int , active: Boolean , fromGiu: Boolean , girlsOnly: Boolean): Ride 
            cancelRide(id: Int!): Ride
            createArea(areaName: String!, distanceFromGiu: Float! , basePrice: Float!): Area
            createSubzone(subzoneName: String! , areaName: String! , subZonePrice: Float!): Subzone
            updateArea(areaName: String! , basePrice: Float! , distanceFromGiu: Float!): Area
            updateSubzone(subzoneName: String! , areaName: String! , subZonePrice: Float!): Subzone
        }
    `;

    const resolvers = {
        DateTime : DateTimeResolver,
        Json : JSONResolver,
        Query: {
            addCookies: async(_parent : any , args : any , {req , res} : any) => {
                res.cookie("Authorization" , args.token , {
                    expires: new Date(Date.now() + 45000000),
                    httpOnly: true,
                    secure: true,
                })
            },
            fetchAllAreas: async(_parent : any , args : any , {req , res} : any) => {
                if(checkAuth(["admin" , "driver" , "student"] , fetchRole(req.headers.cookie)))
                {
                    const areas = await prisma.area.findMany({include : {rides: true , subzones: true}});
                    
                    return areas;
                }
                else
                {
                    throw new Error("Unauthorized");
                }
            },
            fetchRideByCriteria: async(_parent : any , args : any , {req , res} : any) => {
                if(checkAuth(["admin" , "driver" , "student"] , fetchRole(req.headers.cookie)))
                {
                    let rides = await prisma.ride.findMany({
                        where: {
                            areaName : args.areaName,
                            fromGiu : args.fromGiu,
                            girlsOnly : args.girlsOnly
                        }
                    });
                    rides = rides.filter(ride => ride.active === true && ride.seatsLeft > 0);
                    return rides;
                }
                else
                {
                    throw new Error("Unauthorized");
                }
            },
            fetchAllSubzones: async(_parent : any , args : any , {req , res} : any) => {
                if(checkAuth(["admin" , "driver" , "student"] , fetchRole(req.headers.cookie)))
                {
                    const subzones = await prisma.subzone.findMany();
                    return subzones;
                }
                else
                {
                    throw new Error("Unauthorized");
                }
            },
            fetchAllRides: async(_parent : any , args : any , {req , res} : any) => {
                console.log(req.headers.cookie);
                if(checkAuth(["admin" , "driver" , "student"] , fetchRole(req.headers.cookie)))
                {
                    const rides = await prisma.ride.findMany();
                    return rides;
                }
                else
                {
                    throw new Error("Unauthorized");
                }
            },
            fetchMyRides: async(_parent : any , args : any , {req , res} : any) => {
                if(checkAuth(["driver"] , fetchRole(req.headers.cookie)))
                {
                    const rides = await prisma.ride.findMany({
                        where: {
                            driverId: fetchId(req.headers.cookie)
                        }
                    });
                    return rides;
                }
                else
                {
                    throw new Error("Unauthorized");
                }
            },
            fetchRide: async(_parent : any , args : any , {req , res} : any) => {
                if(checkAuth(["admin" , "driver" , "student"] , fetchRole(req.headers.cookie)))
                {
                    const ride = await prisma.ride.findUnique({
                        where: {
                            id : args.id
                        }
                    });
                    return ride;
                }
                else
                {
                    throw new Error("Unauthorized");
                }
            },
            rideCompleted: async(_parent : any , args : any , {req , res} : any) => {
                if(checkAuth(["driver"] , fetchRole(req.headers.cookie)))
                {
                    const ride = await prisma.ride.findUnique({
                        where: {
                            id : args.id
                        }
                    });
                    if(ride === null)
                        throw new Error("Ride not found");

                    if(ride.driverId === fetchId(req.headers.cookie))
                    {
                        const ride = await prisma.ride.update({
                            where: {
                                id : args.id
                            },
                            data : {
                                active : false
                            }
                        });
                        notifyRideCompleted(ride.id);
                        return ride;
                    }
                    else
                    {
                        throw new Error("You are not the driver of this ride");
                    }
                }
                else
                {
                    throw new Error("Unauthorized");
                }
            }
        },
        Mutation: {
            createRide: async(_parent : any , args : any , {req , res} : any) => {
                if(checkAuth(["driver"] , fetchRole(req.headers.cookie)))
                {
                    const userId = fetchId(req.headers.cookie);
                    const area = await prisma.area.findUnique({
                        where: {
                            areaName : args.areaName
                        }
                    });
                    if(area === null)
                        throw new Error("Area not found");

                    const subUser = await prisma.subUser.findFirst({
                        where: {
                            id: fetchId(req.headers.cookie)
                        }
                    });

                    if(subUser === null || subUser === undefined)
                        throw new Error("User not found");

                    if(args.girlsOnly === true && subUser.gender === "male")
                        throw new Error("You cannot create a girls only ride");
                    
                    const carInfo = await prisma.subCar.findFirst({
                        where: {
                            universityId: fetchId(req.headers.cookie).toString()
                        }
                    });
                    
                    if(carInfo === null || carInfo === undefined)
                        throw new Error("You do not have a car");

                    const ride = await prisma.ride.create({
                        data : {
                            driverId : userId,
                            time : args.time,
                            areaName: args.areaName,
                            basePrice : area.basePrice,
                            seatsLeft : carInfo.seats,
                            fromGiu : args.fromGiu,
                            girlsOnly : args.girlsOnly,
                        }
                    });
                    await shareRideDetails(ride);
                    return ride;
                }
                else
                {
                    throw new Error("Unauthorized");
                }
            },
            updateRide: async(_parent : any , args : any , {req , res} : any) => {
                if(checkAuth(["driver"] , fetchRole(req.headers.cookie)))
                {
                    const ride = await prisma.ride.findUnique({
                        where: {
                            id : args.id,
                        }
                    });
                    if(ride === null)
                        throw new Error("Ride not found");

                    if(ride.driverId === fetchId(req.headers.cookie))
                    {
                        const ride = await prisma.ride.update({
                            where: {
                                id : args.id
                            },
                            data : {
                                driverId : args.driverId,
                                time : args.time,
                                areaName: args.areaName,
                                basePrice : args.basePrice,
                                seatsLeft : args.seatsLeft,
                                active : args.active,
                                fromGiu : args.fromGiu,
                                girlsOnly : args.girlsOnly
                            }
                        });
                        return ride;
                    }
                    else
                    {
                        throw new Error("You are not the driver of this ride");
                    }
                }
                else
                {
                    throw new Error("Unauthorized");
                }
            },
            cancelRide: async(_parent : any , args : any , {req , res} : any) => {
                if(checkAuth(["driver"] , fetchRole(req.headers.cookie)))
                {
                    const ride = await prisma.ride.findUnique({
                        where: {
                            id : args.id,
                        }
                    });
                    if(ride === null)
                        throw new Error("Ride not found");

                    if(ride.driverId === fetchId(req.headers.cookie))
                    {
                        const ride = await prisma.ride.update({
                            where: {
                                id : args.id
                            },
                            data : {
                                active : false
                            }
                        });
                        notifyRideCancelled(ride.id);
                        return ride;
                    }
                    else
                    {
                        throw new Error("You are not the driver of this ride");
                    }
                }
                else
                {
                    throw new Error("Unauthorized");
                }
            },
            createArea : async(_parent : any , args : any , {req , res} : any) => {
                if(checkAuth(["admin"] , fetchRole(req.headers.cookie)))
                {
                    const area = await prisma.area.create({
                        data : {
                            areaName : args.areaName,
                            basePrice : args.basePrice,
                            distanceFromGiu : args.distanceFromGiu
                        }
                    });
                    return area;
                }
                else
                {
                    throw new Error("Unauthorized");
                }
            },
            createSubzone : async(_parent : any , args : any , {req , res} : any) => {
                if(checkAuth(["admin"] , fetchRole(req.headers.cookie)))
                {
                    const subzone = await prisma.subzone.create({
                        data : {
                            subzoneName : args.subzoneName,
                            areaName : args.areaName,
                            subZonePrice : args.basePrice
                        }
                    });
                    shareSubzoneDetails(subzone);
                    return subzone;
                }
                else
                {
                    throw new Error("Unauthorized");
                }
            },
            updateArea : async(_parent : any , args : any , {req , res} : any) => {
                if(checkAuth(["admin"] , fetchRole(req.headers.cookie)))
                {
                    const area = await prisma.area.findUnique({
                        where: {
                            areaName : args.areaName
                        }
                    });
                    if(area === null)
                        throw new Error("Area not found");

                    const updatedArea = await prisma.area.update({
                        where: {
                            areaName : args.areaName
                        },
                        data : {
                            basePrice : args.basePrice,
                            distanceFromGiu : args.distanceFromGiu
                        }
                    });
                    return updatedArea;
                }
                else
                {
                    throw new Error("Unauthorized");
                }
            },
            updateSubzone : async(_parent : any , args : any , {req , res} : any) => {
                if(checkAuth(["admin"] , fetchRole(req.headers.cookie)))
                {
                    const subzone = await prisma.subzone.findUnique({
                        where: {
                            subzoneName : args.subzoneName
                        }
                    });
                    if(subzone === null)
                        throw new Error("Subzone not found");

                    const updatedSubzone = await prisma.subzone.update({
                        where: {
                            subzoneName : args.subzoneName
                        },
                        data : {
                            areaName : args.areaName,
                            subZonePrice : args.subZonePrice
                        }
                    });
                    return updatedSubzone;
                }
                else
                {
                    throw new Error("Unauthorized");
                }
            }
        }
    };


    const app = express() as any;
    var corsOptions = {
        origin : "https://giu-pooling-frontend-production.up.railway.app",
        credentials: true
    };
    app.use(cors(corsOptions));

    const server = new ApolloServer({
        typeDefs, 
        resolvers,
        context: async ({req, res}) => ({
            req , res
        }),

    });

    //INTERVAL METHODS
    setInterval(() => {
        periodicDateTimeCheck();
    } , 43200000);

    setInterval(() => {
        periodicTripCompletedCheck();
    } , 3600000);

    await server.start();
    console.log("Server started");
    await server.applyMiddleware({app , path : "/ride" , cors: false});
    console.log("Middleware Applied!");

    app.listen({port : 4001} , () => {
        console.log("Server is ready at http://localhost:4001" + server.graphqlPath);

    })
})();