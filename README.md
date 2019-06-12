### 简介
因一朋友在口腔医院工作，其经常需要统计物品的使用情况，觉得用excel不方便，本人决定免费为其开发一个物品管理工具(至于好不好用，这是后话，哈哈)。该工具主要由物品维护、出入库登记和报表三部分组成。本人初入python，决定使用wxpython开发实现，主要是为了练手，开发过程参照wxpython-demo中的例子，使用到了一些基础控件，比较复杂一点还有TreeCtrl、ListCtrl、Grid等。网上说wxpython是python的首选GUI库，但是发现wxpython的生态并不是很繁荣，github上都没几个项目，不知道是不是搜索方式不对^V^。

![截图1](https://github.com/flyHawk/dentistry/blob/master/SnapShot/001.png)

![截图2](https://github.com/flyHawk/dentistry/blob/master/SnapShot/002.png)
### 运行环境
python 3.7

### 运行方式
```
git clone https://github.com/flyHawk/dentistry.git
cd dentistry
pip install -r requirements.txt
python bootstrap.py
```
默认登录用户和密码均为sysadmin
