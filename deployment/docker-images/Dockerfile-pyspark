FROM eclipse-temurin:17-jdk-jammy
WORKDIR /app
RUN apt-get update && apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install --no-cache-dir pyspark==3.5.0 pandas pyarrow pymongo
COPY processing/pyspark_advanced_03.py /app/pyspark_advanced_03.py
COPY processing/customer_rfm_02.py /app/customer_rfm_02.py
COPY storage/load_to_mongo.py /app/load_to_mongo.py
