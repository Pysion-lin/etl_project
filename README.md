# etl_project
this is a etl and web ui system use python(flask,pandas,APscheduler)

*该项目使用python+pandas+flask+APscheduler

pandas:
    负责从mysql or SQLserver or CSV or txt 等数据源获取数据
    并通过Dataframe转换,处理和装载到mysql中
    
flask:
    是一个与前端交互的处理web服务器(详细请查看ReadAPI.md文件),
    1:前端通过选择和输入数据提取的方式、路径、分割等等的参数;
    2:前端通过选择和输入需要被处理的数据列,和处理的方式,比如数据映射,将具体的某个字段数据映射成另一个数据等方式;
    3:前端通过选择和输入需要这些数据被转载的路径;
    4:前端通过1-3步骤所选择的数据和任务调度的策略进行提交到flask服务器并保存到数据库;
    5:前端通过启动任务,并查看任务状态
 
APscheduler:
    是用来做为任务管理和调度的工具,当flask服务器启动时,APscheduler开启多进程服务进行监控,
    flask服务器处理前端提交的任务参数,通过一系列转换后添加到任务管理器中

后续:
    添加任务错误暂停监控,优化任务处理extract,transform,loader等模块功能

使用案例:
1,启动flask服务器
2,初始化功能模块(触发/api/v1_0/initialize接口 TODO 将其添加到系统启动时自动更新);
3,查询数据提取的接口模块(/api/v1_0/extracter)
4,查询数据转换的接口模块(/api/v1_0/transform)
5,查询数据装载的接口模块(/api/v1_0/loader)
6,根据查询结果组装task任务数据格式和任务调度参数
7,提交任务/api/v1_0/task
8,启动任务/api/v1_0/task/scheduler