# vitals-producer-anomaly-v10

This project generates vital signs data with occasional anomalies and publishes it to a Kafka topic.

## Usage

1.  Configure the `.env` file with your Kafka settings.
2.  Build the Docker image: `docker build -t vitals-producer .`
3.  Run the container: `docker run vitals-producer`