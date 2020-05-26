# rec_sys
我在佛大读书的毕业设计项目，小型推荐系统
主要内容为从新浪新闻爬取新闻，然后进行向量化，之后再求相似度

# 运行环境
Windows Server 2012 (宿主机) Ubuntu 18.04 Server (虚拟机，模拟集群用)
python-3.7
Spark-2.4.3
Scala-2.11
Java8
PostgreSQL11
python依赖的第三方库：requests bs4 web.py psycopg2 

# 配置要点
1. 使用虚拟机模拟集群，每台虚拟机应该安装好Spark-2.4.3，其中内置的Scala为2.11，Java版本为8.
2. 在宿主机使用eclipse打开OfflineCalculater文件夹，即为一个maven项目。使用前应该使用maven package打包出jar文件。在打包过程中，Java依赖的第三方库会自动下载。
3. python应该使用pip安装第三方库，如：pip install requests