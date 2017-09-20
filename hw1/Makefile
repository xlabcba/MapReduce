# Makefile for MapReduce Page Rank project.

# Customize these paths for your environment.
# -----------------------------------------------------------
jar.name=hw1-0.0.1-SNAPSHOT.jar
jar.path=target/${jar.name}
# -----------------------------------------------------------

# Compiles code and builds jar (with dependencies).
jar:
	mvn clean install

calc-sequential:
	java -Xmx1536m -jar ${jar.path} -mode sequential -filename ${FILENAME}

calc-coarselock:
	java -Xmx1536m -jar ${jar.path} -mode coarselock -filename ${FILENAME}

calc-finelock:
	java -Xmx1536m -jar ${jar.path} -mode finelock -filename ${FILENAME}

calc-nosharing:
	java -Xmx1536m -jar ${jar.path} -mode nosharing -filename ${FILENAME}

calc-nolock:
	java -Xmx1536m -jar ${jar.path} -mode nolock -filename ${FILENAME}

calc-all:
	java -Xmx1536m -jar ${jar.path} -mode sequential -filename ${FILENAME}
	java -Xmx1536m -jar ${jar.path} -mode coarselock -filename ${FILENAME}
	java -Xmx1536m -jar ${jar.path} -mode finelock -filename ${FILENAME}
	java -Xmx1536m -jar ${jar.path} -mode nosharing -filename ${FILENAME}
	java -Xmx1536m -jar ${jar.path} -mode nolock -filename ${FILENAME}

calc:
	java -Xmx1536m -jar ${jar.path} -mode ${MODE} -filename ${FILENAME}