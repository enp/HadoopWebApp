FROM openjdk:8
RUN mkdir /opt/hadoop
COPY build/libs/*.jar /opt/hadoop/hadoop.jar
COPY conf /opt/hadoop/conf
WORKDIR /opt/hadoop
CMD ["java", "-jar", "hadoop.jar"]
