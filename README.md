# 简介
	myreplay读取mysql general log， 将提取出来的SQL到指定的数据库执行。 可用于升级版本时回流生产流量来检查是否有不兼容的SQL。
	也可用于回放生产的流量进行压测
![myreplay](https://github.com/GoDannyLai/myreplay/raw/master/misc/img/myreplay.png)

# 使用
  先进行预处理， 生成json文件, 每个json文件包含1000000条SQL:
  ./myreplay -j genlog.json -C 1000000 genlog.log
  
  从上面生成的json文件读入应用到mysql:
  ./myreplay -H 127.0.0.1 -P 3307 -u xx -p xx -d db1 -s dml -c utf8 -i 5 -t 2 -r 600 -J genlog.json.1
	
  自行调整相应的参数
# 限制
	对于像set names utf8这类设置环境变量的SQL跳过不执行。对于不使用db.tb绝对表路径的SQL， 在多线程跑时可能会出错， 
	因为没法对每个线程的mysql连接动态地在执行每个SQL前找到对应的database。执行任何SQL出错都不会中断退出， 直到跑
	完整个general log。 如果要严格不出错， 建议单线程跑。
# 联系
    有任何的bug或者使用反馈， 欢迎联系laijunshou@gmail.com.