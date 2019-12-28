## 中车 IoTDB数据导出为csv文件


### 1.1 环境要求
- Java 8
- Bash

### 1.2 使用步骤

1. 修改conf/config.properties文件, 具体修改方法请看文件中各参数的注释
```
> vim ./conf/config.properties
```
2. 启动导出脚本
```
> ./export-iotdb-csv.sh
```
运行成功后在用户指定的 EXPORT_FILE_DIR 目录下会生成CSV格式的文件
