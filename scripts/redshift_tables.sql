CREATE SCHEMA food_delivery_db;

CREATE TABLE food_delivery_db.dimCustomers (
                CustomerID INT PRIMARY KEY,
                CustomerName VARCHAR(255),
                CustomerEmail VARCHAR(255),
                CustomerPhone VARCHAR(50),
                CustomerAddress VARCHAR(500),
                RegistrationDate DATE
            );

CREATE TABLE food_delivery_db.dimRestaurants (
                RestaurantID INT PRIMARY KEY,
                RestaurantName VARCHAR(255),
                CuisineType VARCHAR(100),
                RestaurantAddress VARCHAR(500),
                RestaurantRating DECIMAL(3,1)
            );

CREATE TABLE food_delivery_db.dimDeliveryDrivers (
                DriverID INT PRIMARY KEY,
                DriverName VARCHAR(255),
                DriverPhone VARCHAR(50),
                DriverVehicleType VARCHAR(50),
                VehicleID VARCHAR(50),
                DriverRating DECIMAL(3,1)
            );

CREATE TABLE food_delivery_db.factOrders (
                OrderID INT PRIMARY KEY,
                CustomerID INT REFERENCES food_delivery_db.dimCustomers(CustomerID),
                RestaurantID INT REFERENCES food_delivery_db.dimRestaurants(RestaurantID),
                DriverID INT REFERENCES food_delivery_db.dimDeliveryDrivers(DriverID),
                OrderDate TIMESTAMP WITHOUT TIME ZONE,
                DeliveryTime INT,
                OrderValue DECIMAL(8,2),
                DeliveryFee DECIMAL(8,2),
                TipAmount DECIMAL(8,2),
                OrderStatus VARCHAR(50)
            );