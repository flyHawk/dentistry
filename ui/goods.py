import wx
import re
import utils
import model
from services import catagoryservice, goodsservice


class AddorEditGoodsDialog(wx.Dialog):

    def __init__(self, parent, title, size=(480,-1)):
        wx.Dialog.__init__(self)
        self.SetExtraStyle(wx.DIALOG_EX_CONTEXTHELP)
        self.Create(parent, wx.ID_ANY, title, wx.DefaultPosition, size , wx.DEFAULT_DIALOG_STYLE, 'AddorEditGoodsDialog')
        self.InitUI()
        self.InitListBoxData()
        
    def InitUI(self):
        
        self.info = wx.InfoBar(self)
        
        name_label = wx.StaticText(self, wx.ID_ANY, '物品名称：')
        unit_label = wx.StaticText(self, wx.ID_ANY, '规格：')
        catagory_label = wx.StaticText(self, wx.ID_ANY, '所属分类：')
        price_label = wx.StaticText(self, wx.ID_ANY, '单价：')
        order_label = wx.StaticText(self, -1, "显示顺序：")
        order_label.SetHelpText('用于控制在物品树中的显示顺序，值越大越靠前')
        
        self.tc_goodsName = wx.TextCtrl(self, wx.ID_ANY,'')
        self.tc_goodsName.SetMaxLength(16)
        self.tc_goodsUnit = wx.TextCtrl(self, wx.ID_ANY, '')
        self.tc_goodsUnit.SetMaxLength(4)
        
        self.tc_goodsPrice = wx.TextCtrl(self, wx.ID_ANY,'')
        self.tc_order = wx.TextCtrl(self,wx.ID_ANY, "1")
        help_msg = '用于控制在物品树中的显示顺序，值越大越靠前'
        order_tp = wx.ToolTip(help_msg)
        self.tc_order.SetToolTip(order_tp)
        self.tc_order.SetMaxLength(6)
        
        self.catagory_lb = wx.ListBox(self, wx.ID_ANY, wx.DefaultPosition, wx.DefaultSize, [], wx.LB_SINGLE|wx.LB_OWNERDRAW)
        gbSizer = wx.GridBagSizer(5,5)
        gbSizer.Add(catagory_label,(0,0), (1, 1),  wx.ALL|wx.ALIGN_RIGHT, 5)  
        gbSizer.Add(self.catagory_lb,  (0,1), (1, 1),  wx.ALL|wx.EXPAND, 5)  
        gbSizer.Add(name_label, (1, 0), (1, 1),  wx.ALL, 5)  
        
        gbSizer.Add(self.tc_goodsName, (1, 1), (1, 1),  wx.ALL|wx.EXPAND, 5)  
        gbSizer.Add(unit_label, (2,0), (1, 1), wx.ALL|wx.ALIGN_RIGHT, 5)      
        gbSizer.Add(self.tc_goodsUnit, (2,1), (1, 1), wx.ALL|wx.EXPAND, 5)     
        
            
        gbSizer.Add(price_label, (3,0), (1, 1),  wx.ALL|wx.ALIGN_RIGHT, 5)      
        gbSizer.Add(self.tc_goodsPrice, (3,1), (1, 1),  wx.ALL|wx.EXPAND, 5)    
        gbSizer.Add(order_label, (4,0), (1, 1),  wx.ALIGN_RIGHT | wx.ALL, 5)        
        gbSizer.Add(self.tc_order, (4,1), (1, 1),  wx.ALL|wx.EXPAND , 5)
        gbSizer.AddGrowableCol(1)
        line = wx.StaticLine(self, -1, size=(20, -1), style=wx.LI_HORIZONTAL)
        
        save_btn = wx.Button(self, wx.ID_OK, label="保存")
        save_btn.SetDefault()
        cancel_btn = wx.Button(self, wx.ID_CANCEL, label="取消")
        hbox = wx.BoxSizer(wx.HORIZONTAL)
        hbox.Add(save_btn, 0, wx.ALL, 5)
        hbox.Add(cancel_btn, 0, wx.ALL, 5)

        self.vBox = wx.BoxSizer(wx.VERTICAL)
        self.vBox.Add(self.info, 0, wx.ALL | wx.EXPAND, 5)
        self.vBox.Add(gbSizer, 0, wx.ALL|wx.EXPAND, 5)
        self.vBox.Add(line, 0, wx.GROW|wx.ALL, 5)
        self.vBox.Add(hbox, 0, wx.ALIGN_CENTER_HORIZONTAL | wx.ALL, 5)
        self.SetSizer(self.vBox)
        
        self.Bind(wx.EVT_BUTTON, self.OnSave, save_btn)
        self.Bind(wx.EVT_BUTTON, self.OnCancel, cancel_btn)
        
        self.tc_goodsName.Bind(wx.EVT_SET_FOCUS, self.OnFocus)
        self.tc_goodsUnit.Bind(wx.EVT_SET_FOCUS, self.OnFocus)
        self.tc_goodsPrice.Bind(wx.EVT_SET_FOCUS, self.OnFocus)
        self.tc_order.Bind(wx.EVT_SET_FOCUS, self.OnFocus)
        self.catagory_lb.Bind(wx.EVT_SET_FOCUS, self.OnFocus)
        
        self.tc_order.Bind(wx.EVT_CHAR, self.OnIntegerChar)
        self.tc_goodsPrice.Bind(wx.EVT_CHAR, self.OnFloatChar)
        
        self.goods_id = -1;
        self.msg =  '添加物品成功!'

    def InitListBoxData(self):
        catagories = catagoryservice.get_all_catagories()
        if catagories : 
            for row in catagories:
                self.catagory_lb.Append(row['CATAGORY_NAME'],   row['ID']);
    
            self.catagory_lb.SetSelection(0)
        else:
            wx.MessageBox("暂无物品类别，请先添加物品类别", "温馨提示", wx.OK_DEFAULT|wx.ICON_WARNING)
            self.Destroy()
        self.vBox.Fit(self)
        
    def OnSave(self, evt):
        # -----------------校验输入值start------------------------------
        goods_name = self.tc_goodsName.GetValue().strip()
        if goods_name == '':
            self.info.ShowMessage('物品名称不能为空', flags=wx.ICON_WARNING);
            return 
        if goodsservice.exists(goods_name, self.goods_id):
            msg = '[{}]该物品已经存在，请重新输入!'.format(goods_name)
            self.info.ShowMessage(msg, wx.ICON_WARNING)
            return 
        
        goods_unit = self.tc_goodsUnit.GetValue().strip()
        if goods_unit == '':
            self.info.ShowMessage('物品规格不能为空！', flags=wx.ICON_WARNING)
            return
    
        goods_order = self.tc_order.GetValue().strip()
        if goods_order == '':
            self.info.ShowMessage('显示顺序不能为空！', flags=wx.ICON_WARNING)
            return 
        pattern = re.compile('^[0-9]+$')
        if re.match(pattern, goods_order) is None:
            self.info.ShowMessage('显示顺序必须是整数数字！', flags=wx.ICON_WARNING)
            return
        
        goods_price = self.tc_goodsPrice.GetValue().strip()
        if goods_price !='':
            pattern= re.compile('^[0-9]*\.?[0-9]{,2}$')
            if re.match(pattern,goods_price ) is None:
                self.info.ShowMessage('单价不符合要求的格式！', flags=wx.ICON_WARNING)
                return
        # -----------------校验输入值end------------------------------
        catagory_id = self.catagory_lb.GetClientData(self.catagory_lb.GetSelection())
        goods_dict = {
                'goods_id': self.goods_id,
                'goods_name': goods_name,
                'goods_order': goods_order,
                'goods_unit' : goods_unit,
                'catagory_id': catagory_id,
                'goods_price': goods_price,
                'save_time': utils.now()
                }
        goodsservice.add_or_update_goods(goods_dict)
        wx.MessageBox(self.msg, '温馨提示', wx.OK_DEFAULT | wx.ICON_INFORMATION)
        # 重新加载物品树
        model.goodsListModel.set(current=goods_name)
        self.Destroy()
        
    def InitCtrlValue(self, goods_id):
        '''
        初始化各控件的值，用于修改物品的时候
        '''
        goodsInfo = goodsservice.get_goods(goods_id)
        self.tc_goodsName.SetValue(goodsInfo['GOODS_NAME'])
        self.tc_goodsPrice.SetValue(str(goodsInfo['GOODS_PRICE']))
        self.tc_goodsUnit.SetValue(goodsInfo['GOODS_UNIT'])
        self.tc_order.SetValue(str(goodsInfo['GOODS_ORDER']))
        
        for n in range(self.catagory_lb.GetCount()):
            if self.catagory_lb.GetString(n) == goodsInfo['CATAGORY_NAME']:
                self.catagory_lb.SetSelection(n)
                break
        # 弹出框消息
        self.msg = '修改物品成功'
        self.goods_id = goods_id
    
    def OnIntegerChar(self, evt):
        key = evt.GetKeyCode()
        if key < wx.WXK_SPACE or key == wx.WXK_DELETE or key > 255:
            evt.Skip()
            return
        if chr(key).isdigit():
            evt.Skip()
            return
        return  
    
    def OnFloatChar(self, evt):
        key = evt.GetKeyCode()
        if key < wx.WXK_SPACE or key == wx.WXK_DELETE or key > 255 or key==46:
            evt.Skip()
            return
        if chr(key).isdigit():
            evt.Skip()
            return
        return
    
    def OnFocus(self, evt):
        self.info.Dismiss()
        evt.Skip()
    
    def OnCancel(self, evt):
        self.Destroy()