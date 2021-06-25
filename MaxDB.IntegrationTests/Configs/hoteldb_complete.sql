//
CREATE USER scott PASSWORD tiger RESOURCE
//
CREATE SCHEMA hotel
//
SET CURRENT_SCHEMA = hotel

//
CREATE TABLE city
(
	zip        CHAR(5)     PRIMARY KEY CONSTRAINT zip_cons CHECK 
                  SUBSTR(zip,1,1) BETWEEN '0' AND '9' AND
                  SUBSTR(zip,2,1) BETWEEN '0' AND '9' AND
                  SUBSTR(zip,3,1) BETWEEN '0' AND '9' AND
                  SUBSTR(zip,4,1) BETWEEN '0' AND '9' AND
                  SUBSTR(zip,5,1) BETWEEN '0' AND '9',
	name       CHAR(20)    NOT NULL,
	state      CHAR(2)     NOT NULL
)

//
CREATE TABLE customer
(
	cno        FIXED(4)    PRIMARY KEY CONSTRAINT cno_cons CHECK cno > 0,
	title      CHAR(7)     CONSTRAINT title_cons CHECK title IN ('Mr', 'Mrs', 'Company'),
	firstname  CHAR(10),
	name       CHAR(10)    NOT NULL,
	zip        CHAR(5)     CONSTRAINT zip_cons CHECK 
                  SUBSTR(zip,1,1) BETWEEN '0' AND '9' AND
                  SUBSTR(zip,2,1) BETWEEN '0' AND '9' AND
                  SUBSTR(zip,3,1) BETWEEN '0' AND '9' AND
                  SUBSTR(zip,4,1) BETWEEN '0' AND '9' AND
                  SUBSTR(zip,5,1) BETWEEN '0' AND '9',
	address    CHAR (25)   NOT NULL,
	FOREIGN KEY customer_zip_in_city (zip) REFERENCES city ON DELETE RESTRICT
)

//
CREATE TABLE hotel
(
	hno        FIXED(4)    PRIMARY KEY CONSTRAINT hno_cons CHECK hno > 0,
	name       CHAR(15)    NOT NULL,
	zip        CHAR(5)     CONSTRAINT zip_cons CHECK
                  SUBSTR(zip,1,1) BETWEEN '0' AND '9' AND
                  SUBSTR(zip,2,1) BETWEEN '0' AND '9' AND
                  SUBSTR(zip,3,1) BETWEEN '0' AND '9' AND
                  SUBSTR(zip,4,1) BETWEEN '0' AND '9' AND
                  SUBSTR(zip,5,1) BETWEEN '0' AND '9',
	address    CHAR(25)    NOT NULL,
	info       LONG,
	FOREIGN KEY hotel_zip_in_city (zip) REFERENCES city ON DELETE RESTRICT
)

//
CREATE TABLE room
(
	hno        FIXED(4)    CONSTRAINT hno_cons CHECK hno > 0,
	type       CHAR(6)     CONSTRAINT type_cons CHECK type IN ('single', 'double', 'suite'), PRIMARY KEY (hno, type),
	free       FIXED(3,0)  CONSTRAINT free_cons CHECK free >= 0,
	price      FIXED(6,2)  CONSTRAINT price_cons CHECK price BETWEEN 0.00 AND 5000.00,
	FOREIGN KEY room_hno_in_hotel (hno) REFERENCES hotel ON DELETE CASCADE
)

//
CREATE TABLE reservation
(
	rno        FIXED(4)    PRIMARY KEY CONSTRAINT rno_cons CHECK rno > 0,
	cno        FIXED(4)    CONSTRAINT cno_cons CHECK cno > 0,
	hno        FIXED(4)    CONSTRAINT hno_cons CHECK hno > 0,
	type       CHAR(6)     CONSTRAINT type_cons CHECK type IN ('single', 'double', 'suite'),
	arrival    DATE        NOT NULL,
	departure  DATE        NOT NULL, CONSTRAINT staying CHECK departure > arrival,
	FOREIGN KEY reservation_cno_in_customer (cno) REFERENCES customer ON DELETE CASCADE,
	FOREIGN KEY reservation_info_in_room (hno,type) REFERENCES room ON DELETE CASCADE
)

//
CREATE TABLE employee
(
	hno        FIXED(4),
	eno        FIXED(4), PRIMARY KEY (hno,eno),
	title      CHAR(7)  CONSTRAINT title_cons CHECk title IN ('Mr', 'Mrs'),
	firstname  CHAR(10),
	name       CHAR(10) NOT NULL,
	manager_eno FIXED(4),
	FOREIGN KEY employee_hno_in_hotel (hno) REFERENCES hotel ON DELETE CASCADE
)

//
//------------------------------------------------------------------
//TABLE city 25 Rows
//------------------------------------------------------------------
//
INSERT INTO city VALUES ('12203', 'Albany', 'NY')
//
INSERT INTO city VALUES ('60601', 'Chicago', 'IL')
//
INSERT INTO city VALUES ('60615', 'Chicago', 'IL')
//
INSERT INTO city VALUES ('45211', 'Cincinnati', 'OH')
//
INSERT INTO city VALUES ('33575', 'Clearwater', 'FL')
//
INSERT INTO city VALUES ('75243', 'Dallas', 'TX')
//
INSERT INTO city VALUES ('32018', 'Daytona Beach', 'FL')
//
INSERT INTO city VALUES ('33441', 'Deerfield Beach', 'FL')
//
INSERT INTO city VALUES ('48226', 'Detroit', 'MI')
//
INSERT INTO city VALUES ('90029', 'Hollywood', 'CA')
//
INSERT INTO city VALUES ('92714', 'Irvine', 'CA')
//
INSERT INTO city VALUES ('90804', 'Long Beach', 'CA')
//
INSERT INTO city VALUES ('11788', 'Long Island', 'NY')
//
INSERT INTO city VALUES ('90018', 'Los Angeles', 'CA')
//
INSERT INTO city VALUES ('70112', 'New Orleans', 'LA')
//
INSERT INTO city VALUES ('10580', 'New York', 'NY')
//
INSERT INTO city VALUES ('10019', 'New York', 'NY')
//
INSERT INTO city VALUES ('92262', 'Palm Springs', 'CA')
//
INSERT INTO city VALUES ('97213', 'Portland', 'OR')
//
INSERT INTO city VALUES ('60018', 'Rosemont', 'IL')
//
INSERT INTO city VALUES ('95054', 'Santa Clara', 'CA')
//
INSERT INTO city VALUES ('20903', 'Silver Spring', 'MD')
//
INSERT INTO city VALUES ('20037', 'Washington', 'DC')
//
INSERT INTO city VALUES ('20005', 'Washington', 'DC')
//
INSERT INTO city VALUES ('20019', 'Washington', 'DC')

//------------------------------------------------------------------
//TABLE customer  15 Rows
//------------------------------------------------------------------
//
INSERT INTO customer VALUES (3000,'Mrs', 'Jenny', 'Porter', '10580', '1340 N.Ash Street, #3')
//
INSERT INTO customer VALUES (3100,'Mr', 'Peter', 'Brown', '48226', '1001 34th Str., APT.3')
//
INSERT INTO customer VALUES (3200,'Company', NULL,'Datasoft', '90018', '486 Maple Str.')
//
INSERT INTO customer VALUES (3300,'Mrs', 'Rose', 'Brian', '75243', '500 Yellowstone Drive, #2')
//
INSERT INTO customer VALUES (3400,'Mrs', 'Mary', 'Griffith', '20005', '3401 Elder Lane')
//
INSERT INTO customer VALUES (3500,'Mr', 'Martin', 'Randolph', '60615', '340 MAIN STREET, #7')
//
INSERT INTO customer VALUES (3600,'Mrs', 'Sally', 'Smith', '75243', '250 Curtis Street')
//
INSERT INTO customer VALUES (3700,'Mr', 'Mike', 'Jackson', '45211', '133 BROADWAY APT. 1')
//
INSERT INTO customer VALUES (3800,'Mrs', 'Rita', 'Doe', '97213', '2000 Humboldt Str., #6')
//
INSERT INTO customer VALUES (3900,'Mr', 'George', 'Howe', '75243', '111 B Parkway, #23')
//
INSERT INTO customer VALUES (4000,'Mr', 'Frank', 'Miller', '95054', '27 5th Str., 76')
//
INSERT INTO customer VALUES (4100,'Mrs', 'Susan', 'Baker', '90018', '200 MAIN STREET, #94')
//
INSERT INTO customer VALUES (4200,'Mr', 'Joseph', 'Peters', '92714', '700 S. Ash Str., APT.12')
//
INSERT INTO customer VALUES (4300,'Company', NULL,'TOOLware', '20019', '410 Mariposa Str., #10')
//
INSERT INTO customer VALUES (4400,'Mr', 'Antony', 'Jenkins', '20903', '55 A Parkway, #15') 

//
//------------------------------------------------------------------
//TABLE hotel  15 Rows
//------------------------------------------------------------------
//
//
INSERT INTO hotel VALUES (10 ,'Congress', '20005', '155 Beechwood Str.', 'Sports and Games
- - - - - - - - - - - -
 
solaria in separate relaxation rooms
sauna, free of charge, individual booking if you 
prefer to sweat alone
a playroom for our small guests
bicycles for our health-conscious guests
storeroom for bicycles in the house
various activities and events every night
broad selection of board games available
indoor swimming pool and fitness center
 
 
Teaching and Learning
- - - - - - - - - - - -
 
Enjoy it. In an absolutely quiet place. The person in charge is available 
during the whole meeting. Consistently high standards providing the ideal 
working environment for you to have a successful meeting.
Telefax and copy machines, even for transparencies.
Daylit rooms creatively arranged.
Aroma lamps with blends of fragrances to improve 
concentration and learning capabilities.
Music in the meeting rooms whenever you like.
 
Are you curious?  We are waiting for you!
')
//
INSERT INTO hotel VALUES (30 ,'Regency', '20037', '477 17th Avenue', 'Our hotel is situated in the city centre in the direct vicinity of the shopping mall.
Our rooms offer you the maximal facilities of a modern middleclass hotel.')
//
INSERT INTO hotel VALUES (20 ,'Long Island', '11788', '1499 Grove Street', NULL)
//
INSERT INTO hotel VALUES (70 ,'Empire State', '12203', '65 Yellowstone Dr.', NULL)
//
INSERT INTO hotel VALUES (80 ,'Midtown', '10019', '12 Barnard Str.', NULL)
//
INSERT INTO hotel VALUES (40 ,'Eight Avenue', '10019', '112 8th Avenue', NULL)
//
INSERT INTO hotel VALUES (50 ,'Lake Michigan', '60601', '354 OAK Terrace', NULL)
//
INSERT INTO hotel VALUES (60 ,'Airport', '60018', '650 C Parkway', 'Welcome in the Airport Hotel.
- - - - - - - - - - - - - - -
 
The Airport Hotel is situated right in the city centre.
Our hotel has spacious, comfortable rooms at attractive prices.
All rooms are with bath/shower, toilet, radio, colour TV, 
video, telephone, minibar and desk.')
//
INSERT INTO hotel VALUES (90 ,'Sunshine', '33575', '200 Yellowstone Dr.', 'The Sunshine is located in the centre of the city.
489 rooms, suites and facilities for the disabled, with 
superb furnishings and genuine elegance are only some of the
features which make this hotel something special.
Individual service ensures personal comfort for all our guests 24 hours a day -
more than just mere luxury.
 
11 function rooms accommodating up to 300 persons are 
available for all types of events.
 
For relaxation and recreation there are a swimming pool with
whirlpool, a sauna, steam bath and solarium.
500 parking spaces are available in the hotel garage.
')
//
INSERT INTO hotel VALUES (100 ,'Beach', '32018', '1980 34th Str.', NULL)
//
INSERT INTO hotel VALUES (110 ,'Atlantic', '33441', '111 78th Str.', NULL)
//
INSERT INTO hotel VALUES (120 ,'Long Beach', '90804', '35 Broadway', NULL)
//
INSERT INTO hotel VALUES (150 ,'Indian Horse', '92262', '16 MAIN STREET', NULL)
//
INSERT INTO hotel VALUES (130 ,'Star', '90029', '13 Beechwood Place', NULL)
//
INSERT INTO hotel VALUES (140 ,'River Boat', '70112', '788 MAIN STREET', NULL)

//
//------------------------------------------------------------------
//TABLE room  38 Rows
//------------------------------------------------------------------
//
//
INSERT INTO room VALUES (10,'single',20,135.00)
//
INSERT INTO room VALUES (10,'double',45,200.00)
//
INSERT INTO room VALUES (30,'single',12,45.00)
//
INSERT INTO room VALUES (30,'double',15,80.00)
//
INSERT INTO room VALUES (20,'single',10,70.00)
//
INSERT INTO room VALUES (20,'double',13,100.00)
//
INSERT INTO room VALUES (70,'single',4,115.00)
//
INSERT INTO room VALUES (70,'double',11,180.00)
//
INSERT INTO room VALUES (80,'single',15,90.00)
//
INSERT INTO room VALUES (80,'double',19,150.00)
//
INSERT INTO room VALUES (80,'suite',5,400.00)
//
INSERT INTO room VALUES (40,'single',20,85.00)
//
INSERT INTO room VALUES (40,'double',35,140.00)
//
INSERT INTO room VALUES (50,'single',50,105.00)
//
INSERT INTO room VALUES (50,'double',230,180.00)
//
INSERT INTO room VALUES (50,'suite',12,500.00)
//
INSERT INTO room VALUES (60,'single',10,120.00)
//
INSERT INTO room VALUES (60,'double',39,200.00)
//
INSERT INTO room VALUES (60,'suite',20,500.00)
//
INSERT INTO room VALUES (90,'single',45,90.00)
//
INSERT INTO room VALUES (90,'double',145,150.00)
//
INSERT INTO room VALUES (90,'suite',60,300.00)
//
INSERT INTO room VALUES (100,'single',11,60.00)
//
INSERT INTO room VALUES (100,'double',24,100.00)
//
INSERT INTO room VALUES (110,'single',2,70.00)
//
INSERT INTO room VALUES (110,'double',10,130.00)
//
INSERT INTO room VALUES (120,'single',34,80.00)
//
INSERT INTO room VALUES (120,'double',78,140.00)
//
INSERT INTO room VALUES (120,'suite',55,350.00)
//
INSERT INTO room VALUES (150,'single',44,100.00)
//
INSERT INTO room VALUES (150,'double',115,190.00)
//
INSERT INTO room VALUES (150,'suite',6,450.00)
//
INSERT INTO room VALUES (130,'single',89,160.00)
//
INSERT INTO room VALUES (130,'double',300,270.00)
//
INSERT INTO room VALUES (130,'suite',100,700.00)
//
INSERT INTO room VALUES (140,'single',10,125.00)
//
INSERT INTO room VALUES (140,'double',9,200.00)
//
INSERT INTO room VALUES (140,'suite',78,600.00)

//
//------------------------------------------------------------------
//TABLE reservation  10 Rows
//------------------------------------------------------------------
//
INSERT INTO reservation VALUES (100,3000,80,'single', '2004-11-13', '2004-11-15')
//
INSERT INTO reservation VALUES (110,3000,100,'double', '2004-12-24', '2005-01-06')
//
INSERT INTO reservation VALUES (120,3200,50,'suite', '2004-11-14', '2004-11-18')
//
INSERT INTO reservation VALUES (130,3900,110,'single', '2005-02-01', '2005-02-03')
//
INSERT INTO reservation VALUES (150,3600,70,'double', '2005-03-14', '2005-03-24')
//
INSERT INTO reservation VALUES (140,4300,80,'double', '2004-04-12', '2004-04-30')
//
INSERT INTO reservation VALUES (160,4100,70,'single', '2004-04-12', '2004-04-15')
//
INSERT INTO reservation VALUES (170,4400,150,'suite', '2004-09-01', '2004-09-03')
//
INSERT INTO reservation VALUES (180,3100,120,'double', '2004-12-23', '2005-01-08')
//
INSERT INTO reservation VALUES (190,4300,140,'double', '2004-11-14', '2004-11-17')
//
//------------------------------------------------------------------
//TABLE employee  10 Rows
//------------------------------------------------------------------
//
INSERT INTO employee VALUES (10,1,'Mrs', 'Martina', 'Bender', NULL)
//
INSERT INTO employee VALUES (10,2,'Mr', 'Martin', 'Bender', NULL)
//
INSERT INTO employee VALUES (10,3,'Mrs', 'Claudia', 'Randolph',1)
//
INSERT INTO employee VALUES (10,4,'Mr', 'Mark', 'Nober',1)
//
INSERT INTO employee VALUES (10,5,'Mrs', 'Phyllis', 'Manger',3)
//
INSERT INTO employee VALUES (10,6,'Mrs', 'Francis', 'Nick',2)
//
INSERT INTO employee VALUES (10,7,'Mr', 'Peter', 'Carley',2)
//
INSERT INTO employee VALUES (10,8,'Mr', 'Ian', 'Wolf', NULL)
//
INSERT INTO employee VALUES (10,9,'Mr', 'Bill', 'Tucker',8)
//
INSERT INTO employee VALUES (10,10,'Mrs', 'Diana', 'Corner',1)

//
//-----------------------------------------------------------------
// CREATE VIEW
//-----------------------------------------------------------------
CREATE VIEW customer_addr (cno, title, name, zip, city, state, address)
  AS SELECT customer.cno, customer.title, customer.name, customer.zip,
            city.name, city.state, customer.address
            FROM   customer, city
            WHERE  customer.zip = city.zip
            WITH CHECK OPTION
//
CREATE VIEW hotel_addr (hno, name, zip, city, state, address)
  AS SELECT hotel.hno, hotel.name, hotel.zip, city.name,
            city.state, hotel.address
            FROM   hotel, city
            WHERE  hotel.zip = city.zip
            WITH CHECK OPTION
//
CREATE VIEW custom_hotel (customname, customcity, hotelname, hotelcity)
  AS SELECT customer_addr.name, customer_addr.city, hotel_addr.name, hotel_addr.city
            FROM  customer_addr,
                  hotel_addr,
                  reservation
            WHERE customer_addr.cno = reservation.cno
            AND   hotel_addr.hno = reservation.hno
//
//
// THE END
