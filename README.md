# hive - Multiline JSON Input Format
JsonInputFormat can read a multiline JSON record in Hive. It is exctrating the record based on balancing of curly braces - { and }. So the content between first '{' to the balanced last '}' is considered as one complete record.

To use it you would need to modify the POJO class (Book.java) according to your JSON Structure or you can modify the parseToString method to parse the JSON record.

input.json is the sample input file.

create table statement:

CREATE TABLE books (id string, bookname string, properties struct<subscription:string, unit:string>) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' COLLECTION ITEMS TERMINATED BY '|' STORED AS INPUTFORMAT 'JsonInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'; 

