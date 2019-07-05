#  Spark

安装spark(需要提前安装Scala)
----------------
+ 下载spark软件包
  	http://mirrors.tuna.tsinghua.edu.cn/apache/spark/spark-2.4.0/spark-2.4.0-bin-hadoop2.7.tgz
+ tar
  	$>tar -xzvf spark-2.4.0-bin-hadoop2.7.tgz -C /app
+ 创建软连接
  	$>cd /app
  	$>ln -sfT spark-2.4.0-bin-hadoop2.7 spark
+ 配置环境变量
  	$>sudo nano /etc/profile 
  	...
  	#spark
  	export SPARK_HOME=/hadoop/home/app/spark
  	export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
+ 生效环境变量
  	$>source /etc/profile
+ 验证spark
  	$>spark-shell

> sc.textFile("file:///home/hadoop/hello.txt").flatMap(_.split(" ")).map((__,1)).reduceByKey(_ ___+ ___)

