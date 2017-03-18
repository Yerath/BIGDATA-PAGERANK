run: jar
	hadoop fs -rm -f -r /user/${USER}

	#Create folders
	hadoop fs -mkdir /user/${USER}
	hadoop fs -chown yerath /user/${USER}
	hadoop fs -mkdir /user/${USER}/pagerank /user/${USER}/pagerank/input 

	#Move the input files
	hadoop fs -rm -f -r /user/${USER}/pagerank/input
	hadoop fs -put graph /user/${USER}/pagerank/input

	#Remove old results
	hadoop fs -rm -f -r  /user/${USER}/pagerank/output
	hadoop jar pagerank.jar nl.hu.hadoop.PageRank /user/${USER}/pagerank/input /user/${USER}/pagerank/output

run_caseSensitive: jar
	hadoop fs -rm -f -r  /user/yerath/pagerank/output
	hadoop jar pagerank.jar nl.hu.hadoop.PageRank -Dwordcount.case.sensitive=true /user/yerath/pagerank/input /user/yerath/pagerank/output 

run_stopwords: jar stopwords
	hadoop fs -rm -f -r  /user/yerath/pagerank/output
	hadoop jar pagerank.jar nl.hu.hadoop.PageRank /user/yerath/pagerank/input /user/yerath/pagerank/output -skip /user/yerath/pagerank/stop_words.text

compile: build/org/myorg/PageRank.class

jar: pagerank.jar

pagerank.jar: build/org/myorg/PageRank.class
	jar -cvf pagerank.jar -C build/ .

build/org/myorg/PageRank.class: PageRank.java
	mkdir -p build
	javac -cp libs/hadoop-mapred-0.22.0.jar:libs/hadoop-common-2.7.3.jar:libs/hadoop-mapreduce-client-core-2.7.3.jar:libs/log4j-1.2.17.jar:libs/asciitable-0.2.5.jar *.java -d build -Xlint

clean:
	rm -rf build pagerank.jar

data:
	hadoop fs -rm -f -r /user/yerath/pagerank/input
	hadoop fs -mkdir /user/yerath/pagerank
	hadoop fs -mkdir /user/yerath/pagerank/input
	echo "Hadoop is an elephant" > file0
	echo "Hadoop is as yellow as can be" > file1
	echo "Oh what a yellow fellow is Hadoop" > file2
	hadoop fs -put file* /user/yerath/pagerank/input
	rm file*

poetry:
	hadoop fs -rm -f -r /user/yerath/pagerank/input
	hadoop fs -mkdir /user/yerath/pagerank/input
	echo -e "Hadoop is the Elephant King! \\nA yellow and elegant thing.\\nHe never forgets\\nUseful data, or lets\\nAn extraneous element cling! "> HadoopPoem0.txt
	echo -e "A wonderful king is Hadoop.\\nThe elephant plays well with Sqoop.\\nBut what helps him to thrive\\nAre Impala, and Hive,\\nAnd HDFS in the group." > HadoopPoem1.txt
	echo -e "Hadoop is an elegant fellow.\\nAn elephant gentle and mellow.\\nHe never gets mad,\\nOr does anything bad,\\nBecause, at his core, he is yellow." > HadoopPoem2.txt
	hadoop fs -put HadoopP* /user/yerath/pagerank/input
	rm HadoopPoem*

showResult:
	hadoop fs -cat /user/yerath/pagerank/output/*
	
stopwords:
	hadoop fs -rm -f /user/yerath/pagerank/stop_words.text
	echo -e "a\\nan\\nand\\nbut\\nis\\nor\\nthe\\nto\\n.\\n," >stop_words.text
	hadoop fs -put stop_words.text /user/yerath/pagerank/

