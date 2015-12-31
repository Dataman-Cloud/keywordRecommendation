###部署文档

#####环境要求
> JAVA: OpenJDK-7
> sbt: 0.13.5
> spark: 1.5.1

#####导入工程所需的依赖
将工程目录下的resource目录的依赖，按照配置文件（src/main/resources/application.conf）导入，并修改配置文件。

#####打包工程
进入工程目录，运行sbt打包命令：
```shell
sbt clean assembly  # 在工程根目录运行
```

#####启动服务
在spark根目录下运行：
```shell
sudo bin/spark-submit --class com.dataman.omega.service.Boot /data/xdcb-service-assembly-1.0.jar
```

####离线模型训练

#####Segment Job
```shell
sudo bin/spark-submit --class com.dataman.nlp.SegmentJob_1 /keywordRecommendation/target/LDApredict-1.0.jar 
```
#####模型训练
```shell
sudo bin/spark-submit --jars ../mysql-connector-java-5.1.36.jar --class com.dataman.nlp.LocalLdaExample /data/NiMeiDe/keywordRecommendation/target/LDApredict-1.0.jar --k 10 --maxIterations 100 --stopwordFile hdfs://10.3.12.9:9000/test/stopword.dic --algorithm online hdfs://10.3.12.9:9000/xxx/article
```

#####预测
```shell
sudo bin/spark-submit --jars ../mysql-connector-java-5.1.36.jar --class com.dataman.nlp.PredictHistory /data/NiMeiDe/keywordRecommendation/target/LDApredict-1.0.jar
```


