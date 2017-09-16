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
	java -Xmx1536m -jar ${jar.path} sequential ${path}

calc-nolock:
	java -Xmx1536m -jar ${jar.path} nolock ${path}

calc-coarselock:
	java -Xmx1536m -jar ${jar.path} coarselock ${path}

calc-finelock:
	java -Xmx1536m -jar ${jar.path} finelock ${path}

calc-nosharing:
	java -Xmx1536m -jar ${jar.path} nosharing ${path}

calc-all:
	java -Xmx1536m -jar ${jar.path} all ${path}

# Package for release.
	