## 中车 KairosDB数据导出为csv文件


### 1.1 环境要求
- Ubuntu 16.04
- Java 8
- Maven
- Git

### 1.2 使用步骤

1. 修改conf/config.properties文件, 具体修改方法请看文件中各参数的注释
```
> vim ./conf/config.properties
```
2. 启动导出脚本
```
> ./export-csv.sh
```
若要自动导出多个车的数据，则使用以下命令
```
> ./multi-device-export-csv.sh
```
修改该脚本中的```DEVICE_ID_LIST```变量可指定车号列表
