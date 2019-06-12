class AbstractModel(object): 
    def __init__(self): 
        self.listeners = [] 
        
    def addListener(self, listenerFunc): 
        self.listeners.append(listenerFunc) 
        
    def removeListener(self, listenerFunc): 
        self.listeners.remove(listenerFunc) 
        
    def update(self): 
        for eachFunc in self.listeners: 
            eachFunc(self)
            
            
class GoodsDetailModel(AbstractModel):
    def __init__(self):
        super(GoodsDetailModel, self).__init__()
        
    def set(self, goods_dict):
        self.data = goods_dict
        self.update()
        
        
class GoodsListModel(AbstractModel):
    def __init__(self):
        super(GoodsListModel, self).__init__()
        
    def set(self, data=None, current=None):
        self.data = data
        self.current = current
        self.update()
        

# 定义共享模型数据
goodsListModel = GoodsListModel()
goodsDeatilModel = GoodsDetailModel()