/* Create the database */
CREATE DATABASE  IF NOT EXISTS FRAUDSDB;

USE FRAUDSDB;

/* Drop existing tables  */
DROP TABLE IF EXISTS fraudtrans;

/* Create the tables */
CREATE TABLE fraudtrans(
Timestamp varchar(100),
TransactionID varchar(100),
AccountID varchar(100), 
Amount DECIMAL(18,2), 
Merchant varchar(100), 
TransactionType varchar(100),
Location varchar(100)
);
