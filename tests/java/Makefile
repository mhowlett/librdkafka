
KAFKA_JARS?=$(KAFKA_PATH)/libs

CLASSES=Murmur2Cli.class TransactionProducerCli.class IncrementalRebalanceCli.class

all: $(CLASSES)

Murmur2Cli.class: Murmur2Cli.java
	javac -classpath $(KAFKA_JARS)/kafka-clients-*.jar $^

TransactionProducerCli.class: TransactionProducerCli.java
	javac -classpath $(KAFKA_JARS)/kafka-clients-*.jar $^

IncrementalRebalanceCli.class: IncrementalRebalanceCli.java
	javac -classpath $(KAFKA_JARS)/kafka-clients-*.jar $^

clean:
	rm -f *.class
