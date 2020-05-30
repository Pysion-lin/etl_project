### 任务接口

## 一,查询数据输入接口
````
请求路径:
    http://192.168.11.31:5000/api/v1_0/extracter
请求方式:
    get
请求参数:
    无
响应数据:
    {
  "data": [
    {
      "args": "['path']",
      "id": 10,
      "name": "get_data_"
    },
    {
      "args": "[]",
      "id": 11,
      "name": "methods"
    },
    {
      "args": "['data']",
      "id": 12,
      "name": "model"
    },
    {
      "args": "['dataframe']",
      "id": 14,
      "name": "process_nall"
    },
    {
      "args": "['dataframe']",
      "id": 15,
      "name": "split_data"
    }
  ],
  "status": 1
}
````
## 二,查询数据转换接口
````
请求路径:
    http://192.168.11.31:5000/api/v1_0/transform
请求方式:
    get
请求参数:
    无
响应数据:
    {
  "data": [
    {
      "args": "['dataframe']",
      "id": 10,
      "name": "load_data"
    },
    {
      "args": "[]",
      "id": 11,
      "name": "methods"
    },
    {
      "args": "['x']",
      "id": 12,
      "name": "save_data"
    }
  ],
  "status": 1
}
````
## 三,查询数据装载接口
````
请求路径:
    http://192.168.11.31:5000/api/v1_0/loader
请求方式:
    get
请求参数:
    无
响应数据:
    {
  "data": [
    {
      "args": "['path']",
      "id": 10,
      "name": "get_data_"
    },
    {
      "args": "[]",
      "id": 11,
      "name": "methods"
    },
    {
      "args": "['data']",
      "id": 12,
      "name": "model"
    },
    {
      "args": "['dataframe']",
      "id": 14,
      "name": "process_nall"
    },
    {
      "args": "['dataframe']",
      "id": 15,
      "name": "split_data"
    }
  ],
  "status": 1
}
````
## 四,任务创建接口
````
请求路径:
    http://192.168.11.31:5000/api/v1_0/task
请求方式:
    post
请求参数:
    {
	"name":"处理SQLserver的任务",
	"extracter":{
		"name":"read_mysql",
		"args":{
			"sql":""
		}
	},
	"transform":{
		"name":"selector",
		"args":{
			"dataframe":"",
			"column":"Sex",
			"from_data":"男",
			"to_data":1
		}
	},
	"loader":{
		"name":"save_data",
		"args":{
			"x":""
		}
	},
	"scheduler":{
		"seconds":5
	}
	
}
响应数据:
    {
  "data": "任务提交成功",
  "status": 1
}
````
## 五,查询任务接口
````
请求路径:
    http://127.0.0.1:5000/api/v1_0/task
请求方式:
    get
请求参数:
    无
响应数据:
    {
  "data": [
    {
      "extract": "read_mysql",
      "extract_parameter": "{'sql': ''}",
      "id": 3,
      "loader": "save_data",
      "loader_parameter": "{'x': ''}",
      "name": "处理SQLserver的任务",
      "scheduler": "5",
      "status": 1,
      "task_id": "20200528162601500",
      "transform": "mapping",
      "transform_parameter": "{'dataframe': '', 'column': 'Sex', 'from_data': '女', 'to_data': 0}"
    }
  ],
  "status": 1
}
````
## 六,启动任务
````
请求路径:
     http://127.0.0.1:5000/api/v1_0/task/scheduler
请求方式:
    post
请求参数:
    {
	
	"id":3
}
响应数据:
{
  "data": "任务启动成功",
  "status": 1
}
````

## 七,查看任务状态
````
与六接口一样:http://127.0.0.1:5000/api/v1_0/task
判断响应数据中的data中的status,如果等于1,表示启动,0表示未启动,-1表示失败
````

## 八.数据源和目标源查询接口
````
请求路径:
    http://127.0.0.1:5000/api/v1_0/resource
请求方法:
    get
请求参数:
    无
响应数据:
{
  "data": [
    {
      "description": {
        "database": "pyetl",
        "ip": "192.168.1.100",
        "password": "12345678",
        "port": 3306,
        "user": "root"
      },
      "id": 1,
      "type": "SQLserver"
    },
    {
      "description": {
        "database": "CRM",
        "ip": "192.168.1.100\\sql2008/",
        "password": "test",
        "user": "test"
      },
      "id": 2,
      "type": "MySQL"
    },
    {
      "description": {
        "header": 0,
        "path": "/root/xxx.csv",
        "sep": ","
      },
      "id": 3,
      "type": "Excel"
    }
  ],
  "status": 1
}
````
## 九.数据源和目标源查询接口
````
请求路径:
     http://127.0.0.1:5000/api/v1_0/resource
请求方式:
    post
请求参数:
{
	"description": 
		{
			"database": "CRM",
			"ip": "192.168.1.100\\sql2008",
			"password": "test",
			"user": "test"
		},
	"id": 2,
	"type": "SQLserver",
	"sql":"select * from tb_bdm_employee limit 1",
	"table":"TB_BDM_Employee"
}
响应数据:
{
  "data": [
    "EmployeeID",
    "EmployeeCode",
    "EmployeeName",
    "BirthDate",
    "HireDate",
    "StateID",
    "AlphabetCode",
    "Sex",
    "PositionID",
    "Address",
    "Tel",
    "EMail",
    "Notes",
    "PostalCode",
    "DeptCode",
    "Version",
    "IsChecker",
    "IsDownLoad",
    "HospitalCode"
  ],
  "status": 1
}
根据查询接口填写对应的数据
````