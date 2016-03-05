# eatclean

This set of Python scripts downloads the latest restaurant health grades from New York City and loads it into a postgres database on Heroku.

The application uses a combintation of Flask and Python scripts deployed on Heroku to parse through the data and upload it into three tables on Postgres. Go to https://eatclean.herokuapp.com/restaurants 

Here are the three tables:

#input_data holds all of the violations made available by NYC. It is essentially the entire dataset NYC provided in a csv file
CREATE TABLE input_data
(
  id serial NOT NULL,
  camis text,
  dba text,
  boro text,
  building text,
  street text,
  zipcode text,
  phone text,
  cuisine_description text,
  inspection_date text,
  action text,
  violation_code text,
  violation_description text,
  critical_flag text,
  score text,
  grade text,
  grade_date text,
  record_date text,
  inspection_type text,
  CONSTRAINT input_data_pkey PRIMARY KEY (id)
)

#restaurant holds all of the eateries inspected by NYC
CREATE TABLE restaurant
(
  camis text NOT NULL,
  dba text,
  boro text,
  building text,
  street text,
  zipcode text,
  phone text,
  cuisine_description text,
  CONSTRAINT restaurant_pkey PRIMARY KEY (camis)
)

#restaurant_grades table holds all of the eateries with a grade issued by NYC
CREATE TABLE restaurant_grades
(
  camis text NOT NULL,
  inspection_date date NOT NULL,
  inspection_type text NOT NULL,
  action text,
  score text,
  grade text NOT NULL,
  grade_date date,
  record_date date,
  CONSTRAINT restaurant_grades_pkey PRIMARY KEY (camis, inspection_date, inspection_type, grade)
)
