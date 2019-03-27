# DataXes
DataXes是基于阿里巴巴开源的[DataX](https://github.com/alibaba/DataX)，二次开发实现的，专门向ElasticSearch离线同步数据的工具。
针对ElasticSearch的独有特性，制定了一套索引命名规则，设计了一套可将同步任务脚本化，并可灵活操作的脚本作业流程。
```
                          ________          __         ____  ___             
                          \______ \ _____ _/  |______  \   \/  /____   ______
                           |    |  \\__  \\   __\__  \  \     _/ __ \ /  ___/
                           |    `   \/ __ \|  |  / __ \_/     \  ___/ \___ \ 
                          /_______  (____  |__| (____  /___/\  \___  /____  >
                                  \/     \/          \/      \_/   \/     \/ 
```

# Features
除了支持DataX本身的特性外，DataXes为同步数据至ElasticSearch扩展了以下特性:

  - 同步作业脚本化，每个作业都使用**python脚本+配置文件**描述，支持一个作业中顺序执行多个子作业
  - JDBC -> ES作业自动构建工具，只需填写jdbc url、sql、es host等必要信息，即可生成可执行的作业脚本
  - 提供了**docker**的运行方式，并强烈推荐使用，真正的达到一键执行，开箱即用
  - 自由配置**增量更新/全量切换**模式，DataXes会将每次作业的执行记录和以时间表示的offset自动记录至目标es
  - 使用es的Index Alias功能，查询将统一使用别名访问索引，全量同步数据可做到**无缝自动切换**当前版本的数据
  - 使用es的Index Template功能，索引配置以模板形式提交至es，建立对应索引时自动套用
  - 引入"**分区**"概念，即通过命名策略，达到同一个索引名(别名)，对应多个分区(索引)的状态
  - 引入"**版本**"概念，即通过命名策略，区分新全量索引与旧索引，全量作业完成时自动将别名切换至新索引，并提供无缝可回滚至旧版本的操作
  - 对es的document操作全面覆盖，支持**index/update/delete/upsert/update by query/delete by query**等等操作，每个子作业可单独配置
  - 可在每条同步数据中，动态配置字段名、索引名(分区)、routing等
  - 内建了lat,lon/wkt等形式转换GeoJson的Transformer
  
# Versions
ESWriter使用目前es正推广的Java Client —— RestClient实现，es 6.2以上版本测试通过，计划持续支持新版本，其余低版本均未测试，会有不同程度的不兼容，暂不考虑进行支持。

# Quick Start

## 工具部署

  * 方法一（推荐，无需配置环境）、通过docker拉取镜像，使用一键命令运行
    1. `docker pull rassyan/dataxes`
    2. `sudo curl -L "https://raw.githubusercontent.com/Rassyan/DataXes/master/dataxes.sh" -o /usr/local/bin/dataxes` 
    3. `sudo chmod +x /usr/local/bin/dataxes` 
    
    完成后即可在任意目录使用
    
   ``` shell
   $ dataxes
   $ cd {YOUR_JOB_HOME} && dataxes {YOUR_JOB_NAME}
   ```

  * 方法二（部署运行）、自己编译、部署
    * System Requirements
        - Linux
        - [JDK(1.8以上，推荐1.8)](http://www.oracle.com/technetwork/cn/java/javase/downloads/index.html)
        - [Apache Maven 3.x](https://maven.apache.org/download.cgi) (Compile DataXes) 
        - [Python(推荐Python 2.7.x)](https://www.python.org/downloads/)
        - python 三方库安装
        
          ``` shell
          pip install jaydebeapi
          pip install pyyaml
          pip install prettytable
          pip install elasticsearch
          ```
    
    1. `git clone https://github.com/Rassyan/DataXes.git`
    2. `cd DataXes && sh package.sh`
    3. `tar zxvf target/datax.tar.gz -C /opt/`
    
    完成后可如下方式使用
    
   ``` shell
   $ python /opt/datax/bin/jdbc_job_tool.py
   $ cd {YOUR_JOB_HOME} && python {YOUR_JOB_NAME}.py
   ```
   
## 运行模式
  * 以通过docker部署的方式说明
  ``` shell
  $ dataxes                                 # 进入Jdbc job tool，进行作业一键生成
  $ dataxes {YOUR_JOB_NAME}                 # 前台进程运行同步作业
  $ dataxes -d {YOUR_JOB_NAME}              # 后台进程运行同步作业，可通过docker命令查看运行状态、日志等
  $ dataxes {YOUR_JOB_NAME} -m auto         # 默认auto方式，可不写，自动判断全量或增量同步作业
  $ dataxes {YOUR_JOB_NAME} -m ff           # 强制运行全量切换方式运行作业，ff也可写作forcefull
  $ dataxes {YOUR_JOB_NAME} -m rb           # 对该作业进行版本回滚，rb也可写作rollback
  $ dataxes {YOUR_JOB_NAME} -m rf           # 对该作业进行版本前滚，rf也可写作rollforward
  $ dataxes {YOUR_JOB_NAME} -e dev          # 运行同步作业，采用dev环境的配置
  $ dataxes {YOUR_JOB_NAME} -e online       # 运行同步作业，采用online环境的配置
  ```

## 支持的数据源
[Datax支持的数据源](https://github.com/alibaba/DataX#support-data-channels)

# Specification
  
## 同步机制
```
first run?  +----+---Y---->  full
                 |
                 |
                 +---N---->  is force full?  +----+---Y---->  full               +---N---->  full
                                                  |                              |
                                                  |                              |
                                                  +---N---->  has inrc job? +----+---Y---->  inrc
```

## 索引命名
  * DataXes通过索引命名策略，引入两个约定的概念 ———— "版本"、"分区"
    
    索引命名约定规则: {$index_name}(@{$partition})@{$version}
    - index_name: 索引名，即作业本身的名字，会作为别名指向以{$index_name}@开头的索引
    - partition: 分区，约定的概念，可有可无，用于使用同一别名扩充索引
    - version: 版本，yyyyMMddhhmmss格式，为作业的end_time
    
    示例
    - user@beijing@20190101000000, 拥有别名user、user@beijing
    - user@shanghai@20181201000000, 拥有别名user、user@shanghai
    
  * DataXes切换别名策略
    - 全量作业
      - 删除es集群中，有临时别名(.{index_name}_new)的索引
      - 指定的索引模板，会附加临时别名(.{index_name}_new)，用于标记哪些索引是全量新生成的索引
      - 索引模板还会附加 refresh_interval=-1, number_of_replicas=0 的配置，用于优化索引的生成
      - 提交索引模板，datax任务启动，子任务依次执行
      - 所有子任务执行完毕，将拥有临时别名(.{index_name}_new)的索引，去掉临时别名并赋予别名，并且将refresh_interval、number_of_replicas恢复模板本身配置，或es默认配置。将之前拥有别名的索引，去掉别名并赋予临时别名(.{index_name}_old)，用于日后版本回滚。同时删除上一批拥有临时别名(.{index_name}_old)的索引
    - 增量作业
      - 除下列两步外，其余与全量作业相同
      - 在datax任务启动前，对目前拥有别名的索引全部赋予临时别名({$index_name}(@{$partition})@{$version})，因为本次作业会将数据写到索引({$index_name}(@{$partition})@{$version})下，采取别名方式使es不创建新索引
      - 在datax任务结束后，对拥有临时别名({$index_name}(@{$partition})@{$version})的索引，删除其临时别名
    - 版本回滚
      - 将拥有临时别名(.{index_name}_old)的索引，去掉临时别名并赋予别名。将之前拥有别名的索引，去掉别名并赋予临时别名(.{index_name}_new)，用于日后版本前滚
    - 版本前滚
      - 将拥有临时别名(.{index_name}_new)的索引，去掉临时别名并赋予别名。将之前拥有别名的索引，去掉别名并赋予临时别名(.{index_name}_old)，用于日后版本回滚。同时删除上一批拥有临时别名(.{index_name}_old)的索引

# Communication
DataXes的QQ交流群220616594