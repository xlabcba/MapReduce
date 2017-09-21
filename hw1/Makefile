# Makefile for Multi-Version of Weather Data Analysis

# Customize these paths for your environment.
# -----------------------------------------------------------
jar.name=hw1-0.0.1-SNAPSHOT.jar
jar.path=target/${jar.name}
input.name=1912.csv.gz
input.path=input/${input.name}
# -----------------------------------------------------------

# Compiles code and builds jar (with dependencies).
jar:
	mvn clean package

sequential:
	java -Xmx1536m -jar ${jar.path} -input ${input.path} -mode sequential 

nolock:
	java -Xmx1536m -jar ${jar.path} -input ${input.path} -mode nolock 

coarselock:
	java -Xmx1536m -jar ${jar.path} -input ${input.path} -mode coarselock 

finelock:
	java -Xmx1536m -jar ${jar.path} -input ${input.path} -mode finelock 

nosharing:
	java -Xmx1536m -jar ${jar.path} -input ${input.path} -mode nosharing 

all: sequential nolock coarselock finelock nosharing