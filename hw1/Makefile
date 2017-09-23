# Makefile for Multi-Version of Weather Data Analysis

# Customize these paths and flags for your environment and your running condition.
# -----------------------------------------------------------
jar.name=hw1-0.0.1-SNAPSHOT.jar
jar.path=target/${jar.name}
input.name=1912.csv.gz
input.path=input/${input.name}
# Fibonacci calculation switch (true/false)
fibo.on=true
# Debug switches (true/false) 
# If true, print averages after each run
debug.printAvg=true
# If true, comparing averages with sequential result after each run 
# (Ignored in sequential version)
debug.checkAvg=false
# -----------------------------------------------------------

# Compiles code and builds jar (with dependencies).
jar:
	mvn clean package

sequential:
	java -Xmx1536m -jar ${jar.path} -input ${input.path} -mode sequential -fibo ${fibo.on} -printAvg ${debug.printAvg} -checkAvg ${debug.checkAvg}

nolock:
	java -Xmx1536m -jar ${jar.path} -input ${input.path} -mode nolock -fibo ${fibo.on} -printAvg ${debug.printAvg} -checkAvg ${debug.checkAvg}

coarselock:
	java -Xmx1536m -jar ${jar.path} -input ${input.path} -mode coarselock -fibo ${fibo.on} -printAvg ${debug.printAvg} -checkAvg ${debug.checkAvg}

finelock:
	java -Xmx1536m -jar ${jar.path} -input ${input.path} -mode finelock -fibo ${fibo.on} -printAvg ${debug.printAvg} -checkAvg ${debug.checkAvg}

nosharing:
	java -Xmx1536m -jar ${jar.path} -input ${input.path} -mode nosharing -fibo ${fibo.on} -printAvg ${debug.printAvg} -checkAvg ${debug.checkAvg}

all: sequential nolock coarselock finelock nosharing