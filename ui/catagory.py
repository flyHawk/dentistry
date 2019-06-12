import wx
import re
import utils
import model
from services import catagoryservice


class AddorEditCatagoryDialog(wx.Dialog):

    def __init__(self, parent, sid, title, size=(350, 296), pos=wx.DefaultPosition,
                 style=wx.DEFAULT_DIALOG_STYLE, name='dialog'):

        wx.Dialog.__init__(self)
        self.SetExtraStyle(wx.DIALOG_EX_CONTEXTHELP)
        self.Create(parent, sid, title, pos, size, style, name)
        
        self.info = wx.InfoBar(self)
        
        help_msg = '用于控制在物品树中的显示顺序，值越大越靠前'
        order_tp = wx.ToolTip(help_msg)
        name_label = wx.StaticText(self, -1, "类别名称：")
        order_label = wx.StaticText(self, -1, "显示顺序：")
        order_label.SetHelpText(help_msg)
        desc_label = wx.StaticText(self, -1, "类别描述：")
        
        self.tc_catagoryname = wx.TextCtrl(self, -1, "")
        self.tc_catagoryname.SetMaxLength(12)
        self.tc_order = wx.TextCtrl(self, -1, "1")
        self.tc_order.SetMaxLength(6)
        self.tc_order.SetToolTip(order_tp)
        self.tc_catagorydesc = wx.TextCtrl(self, -1, "", style=wx.TE_MULTILINE, size=(-1, 100))

        fgSizer = wx.FlexGridSizer(3, 2, 5, 5)
        fgSizer.Add(name_label, 0, wx.ALIGN_RIGHT | wx.ALL, 5)        
        fgSizer.Add(self.tc_catagoryname, 1, wx.ALL | wx.EXPAND, 5)       
        fgSizer.Add(order_label, 0, wx.ALIGN_RIGHT | wx.ALL, 5)       
        fgSizer.Add(self.tc_order, 0, wx.ALL | wx.EXPAND, 5)        
        fgSizer.Add(desc_label, 0, wx.ALIGN_RIGHT | wx.ALL, 5)       
        fgSizer.Add(self.tc_catagorydesc, 1, wx.ALL | wx.EXPAND, 5)        
        fgSizer.AddGrowableCol(1, proportion=1)

        line = wx.StaticLine(self, -1, size=(20, -1), style=wx.LI_HORIZONTAL)
        
        save_btn = wx.Button(self, wx.ID_OK, label="保存")
        save_btn.SetDefault()
        cancel_btn = wx.Button(self, wx.ID_CANCEL, label="取消")
        hbox = wx.BoxSizer(wx.HORIZONTAL)
        hbox.Add(save_btn, 0, wx.ALL, 5)
        hbox.Add(cancel_btn, 0, wx.ALL, 5)

        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(self.info, 0, wx.EXPAND, 5)
        sizer.Add(fgSizer, 0, wx.ALL | wx.EXPAND, 5)        
        sizer.Add(line, 0, wx.EXPAND | wx.ALL)
        sizer.Add(hbox, 0, wx.ALIGN_CENTER_HORIZONTAL | wx.ALL, 5)
        self.SetSizer(sizer)

        self.Bind(wx.EVT_BUTTON, self.OnSave, save_btn)
        self.Bind(wx.EVT_BUTTON, self.OnCancel, cancel_btn)
        
        self.tc_order.Bind(wx.EVT_CHAR, self.OnChar)
        
        self.tc_catagoryname.Bind(wx.EVT_SET_FOCUS, self.OnFocus)
        self.tc_order.Bind(wx.EVT_SET_FOCUS, self.OnFocus)
        self.tc_catagorydesc.Bind(wx.EVT_SET_FOCUS, self.OnFocus)
        
        self.catagory_id = -1;
        self.msg =  '添加物品类别成功!'
        
    def OnSave(self, evt):
        # -----------------校验输入值start------------------------------
        catagory_name = self.tc_catagoryname.GetValue().strip()
        if catagory_name == '':
            self.info.ShowMessage('物品类别不能为空！', flags=wx.ICON_WARNING)
            return 
        if catagoryservice.exists(catagory_name, self.catagory_id):
            msg = '[{}]该物品类别已经存在，请重新输入!'.format(catagory_name)
            self.info.ShowMessage(msg, wx.ICON_WARNING)
            return 
        
        catagory_order = self.tc_order.GetValue().strip()
        if catagory_order == '':
            self.info.ShowMessage('显示顺序不能为空！', flags=wx.ICON_WARNING)
            return 
        pattern = re.compile('^[0-9]+$')
        if re.match(pattern, catagory_order) is None:
            self.info.ShowMessage('显示顺序必须是整数数字！', flags=wx.ICON_WARNING)
            return
        # -----------------校验输入值end------------------------------
        catagory_dict = {
                'catagory_id': self.catagory_id,
                'catagory_name': catagory_name,
                'catagory_order': catagory_order,
                'catagory_desc' : self.tc_catagorydesc.GetValue().strip(),
                'save_time': utils.now()
                }
        catagoryservice.add_or_update_catagory(catagory_dict)
        wx.MessageBox(self.msg, '温馨提示', wx.OK_DEFAULT | wx.ICON_INFORMATION)
        # 重新加载物品树
        model.goodsListModel.set(current=catagory_name)
        self.Destroy()
            
    def OnChar(self, evt):
        key = evt.GetKeyCode()
        if key < wx.WXK_SPACE or key == wx.WXK_DELETE or key > 255:
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
        
    def InitCtrlValue(self, catagory_id):
        '''
        初始化各控件的值，用于修改物品类别的时候
        '''
        catagory = catagoryservice.get_catagory(catagory_id)
        self.tc_catagoryname.SetValue(str(catagory['CATAGORY_NAME']))
        self.tc_catagorydesc.SetValue(str(catagory['CATAGORY_DESC']))
        self.tc_order.SetValue(str(catagory['CATAGORY_ORDER']))
        self.catagory_id = catagory_id
        self.msg =  '修改物品类别成功!'
