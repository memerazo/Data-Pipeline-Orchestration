FROM apache/airflow:2.9.0

USER root

# Instalar OpenJDK-17 y herramientas necesarias, incluido wget
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk ant procps wget && \
    apt-get clean

# Configurar JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64/

# Crear el directorio /opt/bitnami/spark/jars/
RUN mkdir -p /opt/bitnami/spark/jars/


USER airflow

# Copiar el archivo requirements.txt
COPY requirements.txt /tmp/requirements.txt

# Instalar paquetes desde el archivo requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER root

# Descargar y copiar los JARs necesarios para Hadoop, AWS SDK y Guava
RUN wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar -P /opt/bitnami/spark/jars/ && \
    wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk/1.11.1000/aws-java-sdk-1.11.1000.jar -P /opt/bitnami/spark/jars/ && \
    wget https://repo1.maven.org/maven2/com/google/guava/guava/30.1.1-jre/guava-30.1.1-jre.jar -P /opt/bitnami/spark/jars/ && \
    wget https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.2.0/hadoop-common-3.2.0.jar -P /opt/bitnami/spark/jars/

USER airflow