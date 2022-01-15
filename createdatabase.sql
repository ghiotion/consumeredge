CREATE TABLE companies(
   company_id int PRIMARY KEY,
   ticker char(5),
   name char(100)
);

CREATE TABLE historical(
  company_id int,
  open decimal(10,2),
  close decimal(10,2),
  high decimal(10,2),
  low  decimal(10,2),
  volume int, 
  date date
);