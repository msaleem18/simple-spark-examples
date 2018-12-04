# simple-spark-examples

### Odd / Even Numbers

```python

#creating RDD from the text file 
int_list = sc.textFile("/user/root/assign5/integer_list.txt") #load text file from hdfs
 
#flatmap RDD to split the numbers and then convert them to int format 
int_list_map = int_list.flatMap(lambda line: line.split()) 
int_list_map_int = int_list_map.map(lambda num: int(num)) 
 
#filter RDD to get only even numbers 
int_list_map_int_even = int_list_map_int.filter(lambda x: x%2 == 0) 
 
#filter RDD to get only odd numbers 
int_list_map_int_odd = int_list_map_int.filter(lambda x: x%2 != 0) 
 
#print results 
print("even count: %s" % int_list_map_int_even.count()) 
print("odd count: %s" % int_list_map_int_odd.count())
```

### Salary Sum by Deparatment

```python

#creating RDD from the text file 
dept_salary= sc.textFile("/user/root/assign5/dept_salary.txt") 

#flatmap RDD to split the the lines and then map them again to create key,val pairs 
dept_salary_map = dept_salary.flatMap(lambda line: line.split('\n')) 
dept_salary_map2 = dept_salary_map.map(lambda line: (line.split(" ")[0],int(line.split(" ")[1]))) 
dept_salary_map_byKey = dept_salary_map2.reduceByKey(lambda x,y: x+y) 

#print results 
print(dept_salary_map_byKey.collect())
```

### Top 10 and Bottom 10 words

```python

#creating RDD from the text file 
shake_data= sc.textFile("/user/root/assign5/shakespeare_100.txt") 

#flatmap RDD to split the the lines and then map them again to create key,val pairs of each word 
shake_data_map = shake_data.flatMap(lambda line: line.split()).map(lambda word: (word,1)) 
shake_data_keyVal = shake_data_map.reduceByKey(lambda x,y: x+y) 

#invert the key value pairs, so that the value is key now 
shake_data_keyVal_inverted = shake_data_keyVal.map(lambda x: (x[1],x[0])) 

#sort based on value 
shake_data_keyVal_inverted_ASC = shake_data_keyVal_inverted.sortByKey() 
shake_data_keyVal_inverted_DSC = shake_data_keyVal_inverted.sortByKey(ascending=False) 

#print results 
print("ascending results") 
print(shake_data_keyVal_inverted_ASC.take(10)) 
print("descendingresults") 
print(shake_data_keyVal_inverted_DSC.take(10))
```
