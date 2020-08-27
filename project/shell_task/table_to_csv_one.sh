#!/bin/bash
chmod +x '$1'
hive --database '$4' -e 'set hive.cli.print.header=true; select average_value,name,ratings,total_rating from final1'| sed 's/[\t]/,/g'> '$2'/top_10_popular_mobile_phones.csv
hive --database '$4' -e 'set hive.cli.print.header=true; select name,price,catagory from final21' | sed 's/[\t]/,/g'> '$2'/top_5_mobiles_phones_categorywise.csv
hive --database '$4' -e 'set hive.cli.print.header=true; select median,name,price,ratings,total_rating from final3' | sed 's/[\t]/,/g'> '$2'/top_10_recommended_mobile.csv
hive --database '$4' -e 'set hive.cli.print.header=true; select brand,name,ram,rom from final4' |sed 's/[\t]/,/g'> '$2'/best_configuration_phone_categorywise.csv
hdfs dfs -put '$2'/top_10_popular_mobile_phones.csv'$3'
hdfs dfs -put '$2'/top_5_mobiles_phones_categorywise.csv'$3'
hdfs dfs -put '$2'/top_10_recommended_mobile.csv'$3'
hdfs dfs -put '$2'/best_configuration_phone_categorywise.csv'$3'
