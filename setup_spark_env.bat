@echo off
setx JAVA_HOME "C:\Program Files\OpenJDK\jdk-11"
setx SPARK_HOME "%USERPROFILE%\.local\lib\python3.11\site-packages\pyspark"
setx HADOOP_HOME "%SPARK_HOME%\hadoop"
setx PATH "%JAVA_HOME%\bin;%SPARK_HOME%\bin;%HADOOP_HOME%\bin;%PATH%"
setx PYTHONPATH "%SPARK_HOME%\python;%SPARK_HOME%\python\lib\py4j-0.10.9.7-src.zip;%PYTHONPATH%"

echo Environment variables set. Please restart your terminal for changes to take effect.
pause 