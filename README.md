# Please add your team members' names here. 

## Team members' names 

1. Student Name:

   Student UT EID:

2. Student Name:

   Student UT EID:

 ...

##  Course Name: CS378 - Cloud Computing 

##  Unique Number: 51515
    


# Add your Project REPORT HERE 


# Project Template

This is a Java Maven Project Template


# How to compile the project

We use Apache Maven to compile and run this project. 

You need to install Apache Maven (https://maven.apache.org/)  on your system. 

Type on the command line: 

```bash
mvn clean compile
```

# How to create a binary runnable package 


```bash
mvn clean compile assembly:single
```


# How to run the Server 

```bash
mvn clean  compile  exec:java@server -Dexec.args="33333"
```



# How to run the Client

```bash
mvn clean  compile  exec:java@client  -Dexec.args="2000000 localhost 33333"
```

Change the localhost with the local ip address of your cloud machine. 


Data batch size is: 2000000

You can modify the batch size based on available memory.


If you run this over SSH and it takes a lots of time you can run it in background using the following command

```bash
nohub mvn clean  compile  exec:java@client  -Dexec.args="2000000 localhost 33333"  & 
```

We recommend the above command for running the Main Java executable. 







