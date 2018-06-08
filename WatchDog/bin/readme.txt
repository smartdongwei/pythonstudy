新建目录
mkdir /opt/sanhui/watchdog

文件全部拷贝到 
cp * /opt/sanhui/watchdog
cd /opt/sanhui/watchdog

转换脚本换行符
./dos2unix installservice.sh
./dos2unix uninstallservice.sh

安装服务
./installservice.sh

启动服务
service watchdog start
service watchdog status

