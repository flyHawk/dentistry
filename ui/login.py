'''
-------------------system module-------------------
'''
import wx

'''
-------------------user module---------------------
'''
import images

from ui.main import MainFrame
from services import userservice


class LoginDialog(wx.Dialog):
    
    def __init__(self):
        
        super(LoginDialog, self).__init__(None, wx.ID_ANY, title='登录', size=(550, 320))
        # 初始化界面
        self.InitUI()
        # 窗口居中
        self.Center()
        
    def InitUI(self):
        
        # 控件
        logo_bm = wx.StaticBitmap(self, wx.ID_ANY, images.tee.GetBitmap())
        
        sysname_lb = wx.StaticText(self, wx.ID_ANY, u"牙科物品出入库管理系统")
        sysname_lb.SetFont(wx.Font(18, 70, 90, 92, False, wx.EmptyString))
        account_lb = wx.StaticText(self, wx.ID_ANY, u"账户：")
        password_lb = wx.StaticText(self, wx.ID_ANY, u"密码：")
        version_lb = wx.StaticText(self, wx.ID_ANY, u"v1.0 Created By crazeblade,20190512")
        
        self.account_tc = wx.TextCtrl(self, wx.ID_ANY, wx.EmptyString, size=(150, -1))
        self.account_tc.SetMaxLength(15)
        self.password_tc = wx.TextCtrl(self, wx.ID_ANY, wx.EmptyString, size=(150, -1), style=wx.TE_PASSWORD)
        self.password_tc.SetMaxLength(15)
        
        logon_btn = wx.Button(self, wx.ID_ANY, '登录')
        self.Bind(wx.EVT_BUTTON, self.OnLogin, logon_btn);
    
        # 布局
        bSizer = wx.BoxSizer(wx.VERTICAL)
        
        gbSizer = wx.GridBagSizer(20, 10)
        gbSizer.SetFlexibleDirection(wx.BOTH)
        gbSizer.SetNonFlexibleGrowMode(wx.FLEX_GROWMODE_SPECIFIED)

        gbSizer.Add(logo_bm, wx.GBPosition(1, 1), wx.GBSpan(4, 1), wx.ALL, 5)
        gbSizer.Add(sysname_lb, wx.GBPosition(1, 2), wx.GBSpan(1, 2), wx.ALL, 5)
        gbSizer.Add(account_lb, wx.GBPosition(2, 2), wx.GBSpan(1, 1), wx.ALL, 5)
        gbSizer.Add(self.account_tc, wx.GBPosition(2, 3), wx.GBSpan(1, 1), wx.ALL, 5)
        gbSizer.Add(password_lb, wx.GBPosition(3, 2), wx.GBSpan(1, 1), wx.ALL, 5)
        gbSizer.Add(self.password_tc, wx.GBPosition(3, 3), wx.GBSpan(1, 1), wx.ALL, 5)
        gbSizer.Add(logon_btn, wx.GBPosition(4, 3), wx.GBSpan(1, 1), wx.ALL, 5)
        
        gbSizer.AddGrowableRow(4)
        
        bSizer.Add(gbSizer, 1, wx.EXPAND, 5)
        bSizer.Add(version_lb, 0, wx.ALIGN_RIGHT | wx.ALL, 5)

        self.SetSizer(bSizer)

    def OnLogin(self, evt):
        # 输入校验
        username = self.account_tc.GetValue()
        if not username.strip():
            wx.MessageBox("账户不能为空！", "温馨提示", wx.YES_DEFAULT | wx.ICON_EXCLAMATION)
            self.account_tc.SetFocus()
            return

        password = self.password_tc.GetValue()
        if not password.strip():
            wx.MessageBox("密码不能为空！", "温馨提示", wx.YES_DEFAULT | wx.ICON_EXCLAMATION)
            self.password_tc.SetFocus()
            return
        
        if userservice.auth(username, password) :
            self.Close(True)
            frm = MainFrame()
            frm.CenterOnScreen(direction=wx.BOTH)
            frm.Show()
        else:
            wx.MessageBox("用户名或者密码不正确！", "温馨提示", wx.YES_DEFAULT | wx.ICON_EXCLAMATION)
