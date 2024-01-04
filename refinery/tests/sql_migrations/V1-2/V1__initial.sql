CREATE SEQUENCE persons_id_seq;

CREATE TABLE persons(
  id integer PRIMARY KEY DEFAULT nextval('persons_id_seq'),
  name varchar(255),
  city varchar(255)
);
