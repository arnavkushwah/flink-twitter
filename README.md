# Real-Time Event Summarization on Twitter Using Apache Flink & Kafka



---

##  Prerequisites: Install Required Software

### Install Java 8+
Apache Flink requires Java. Install OpenJDK:
```sh
brew install openjdk@17  # For Mac
sudo apt install openjdk-17-jdk  # For Ubuntu
```
### Install Apache Maven
Used for building and managing Java dependencies:
```sh
brew install maven  # For Mac
sudo apt install maven  # For Ubuntu
```

### Install Apache Kafka
Kafka is used for streaming tweets:
```sh
brew install kafka  # For Mac
sudo apt install kafka  # For Ubuntu
```

### Ensure Python 3.11 is installed:
```sh
python3.11 --version
```

### Then create a virtual environment:
```sh
cd flink-twitter/src/main/python
python3.11 -m venv venv
source venv/bin/activate  # Mac/Linux
venv\\Scripts\\activate  # Windows
```

### Install Required Python Packages
```sh
pip install -r requirements.txt
```

in the driectory src/main/python


## Setting Up Twitter and OpenAI API Keys
Add API Credentials to twitter_producer.py

Edit src/main/python/twitter_producer.py and replace:
```sh
BEARER_TOKEN = "your-bearer-token"
```
```sh
OPENAI_KEY = "ENTER KEY HERE"
```

## Running the Pipeline
## Start Zookeeper & Kafka
Kafka requires Zookeeper. Open two terminals and run:
Terminal 1(Start Zookeeper)
```sh
zookeeper-server-start /opt/homebrew/etc/kafka/zookeeper.properties
```
Terminal 2 (Start Kafka)
```sh
kafka-server-start /opt/homebrew/etc/kafka/server.properties
```

Verify Kafka Topics:
```sh
kafka-topics --list --bootstrap-server localhost:9092
```
If twitter-stream doesn’t exist, create it:
```sh
kafka-topics --create --topic twitter-stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

## Run the Twitter Producer (Fetch Tweets & Send to Kafka)
```sh
cd flink-twitter/src/main/python
source venv/bin/activate  # Mac/Linux
python twitter_producer.py
```

## Run the Frontend
Run the following command in the src/main/python directory 

streamlit run frontend_app.py

## Test timings and results of flink pipeline
Run the following command in the src/main/python directory 

python flink_proocessor.py --input output-[num tweets].jsonl

and choose any num tweets size available in the same directory. 

Uncomment timing code in flink_processor.py.



For CS214
 
