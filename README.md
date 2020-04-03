# Big Data Systems homework

Svi demoni (Kafka, Spark, HDFS i Cassandra) su pokrenuti. Ako se ipak ispostavi da su u medjuvremenu zaustavljeni, pokrecu se skriptom start-all.sh (svi osim HDFS-a);

Kada se Cassandra pokrene, potrebno je izvrsiti skriptu spark-project/cassandra-commands.py kako bi se napravile neophodne tabele;

Aplikacija koja cita record-e i salje ih na kafka topic za projekat 2 je kafka-sender1.py, a za projekat 3 je kafka-sender2.py;



Projekat 2 se pokrece komandama:


python kafka-sender1.py


spark-submit --master spark://localhost:7077 --jars spark-streaming-kafka-0-8-assembly_2.11-2.4.5.jar app-streaming.py ;



Projekat 3 se pokrece komandama:


python kafka-sender2.py


spark-submit --master spark://localhost:7077 --jars spark-streaming-kafka-0-8-assembly_2.11-2.4.5.jar app-ml.py ;



Sve komande se pokrecu iz spark-project foldera.
