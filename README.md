# hive - Multiline JSON Input Format
JsonInputFormat can read a multiline JSON record in Hive. It is exctrating the record based on balancing of curly braces - { and }. So the content between first '{' to the balanced last '}' is considered as one complete record.

This InputFormat can be used along with a Json Serde to read a multiline JSON file.

'input.json' is a sample input file. Sample create table statement to read this file:

CREATE TABLE books (id string, bookname string, properties struct<subscription:string, unit:string>) ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe' STORED AS INPUTFORMAT 'JsonInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'; 

