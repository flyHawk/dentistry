import wx
from services import userservice


class ResetLoginPasswdDialog(wx.Dialog):

    def __init__(self, parent, sid, title, size=(350, 225), pos=wx.DefaultPosition,
                 style=wx.DEFAULT_DIALOG_STYLE, name='dialog'
                 ):
        wx.Dialog.__init__(self)
        self.Create(parent, sid, title, pos, size, style, name)
        
        self.info = wx.InfoBar(self)

        oldpassword_label = wx.StaticText(self, -1, "原登录密码：")
        newpassword_label = wx.StaticText(self, -1, "新密码：")
        comfirm_label = wx.StaticText(self, -1, "新密码确认：")
        
        self.tc_oldpasswd = wx.TextCtrl(
            self, -1, "", style=wx.TE_PASSWORD,)
        self.tc_oldpasswd.SetMaxLength(15)
        self.tc_newpasswd = wx.TextCtrl(
            self, -1, "", style=wx.TE_PASSWORD)
        self.tc_newpasswd.SetMaxLength(15)
        self.tc_comfirmpasswd = wx.TextCtrl(
            self, -1, "", style=wx.TE_PASSWORD)
        self.tc_comfirmpasswd.SetMaxLength(15)
        
        fgSizer = wx.FlexGridSizer(3, 2, 5, 5)
        fgSizer.Add(oldpassword_label, 0, wx.ALIGN_RIGHT | wx.ALL, 5)
        fgSizer.Add(self.tc_oldpasswd, 1, wx.ALL | wx.EXPAND, 5)
        fgSizer.Add(newpassword_label, 0, wx.ALIGN_RIGHT | wx.ALL, 5)       
        fgSizer.Add(self.tc_newpasswd, 1, wx.ALL | wx.EXPAND, 5)
        fgSizer.Add(comfirm_label, 0, wx.ALIGN_RIGHT | wx.ALL, 5)
        fgSizer.Add(self.tc_comfirmpasswd, 1, wx.ALL | wx.EXPAND, 5)
        fgSizer.AddGrowableCol(1)

        line = wx.StaticLine(self, -1, size=(20, -1), style=wx.LI_HORIZONTAL)
        
        save_btn = wx.Button(self, wx.ID_OK, label="保存")
        save_btn.SetDefault()
        cancel_btn = wx.Button(self, wx.ID_CANCEL, label="取消")
        hbox = wx.BoxSizer(wx.HORIZONTAL)
        hbox.Add(save_btn, 0, wx.ALL, 5)
        hbox.Add(cancel_btn, 0, wx.ALL, 5)
        
        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(self.info, 0, wx.ALL | wx.EXPAND, 5)
        sizer.Add(fgSizer, 0, wx.ALL | wx.EXPAND, 5)
        sizer.Add(line, 0, wx.ALL | wx.EXPAND, 5)
        sizer.Add(hbox, 0, wx.ALL | wx.ALIGN_CENTER_HORIZONTAL, 5)
        
        self.SetSizer(sizer)

        self.Bind(wx.EVT_BUTTON, self.OnSave, save_btn)
        self.Bind(wx.EVT_BUTTON, self.OnCancel, cancel_btn)
        
        self.tc_oldpasswd.Bind(wx.EVT_SET_FOCUS, self.OnFocus)
        self.tc_newpasswd.Bind(wx.EVT_SET_FOCUS, self.OnFocus)
        self.tc_comfirmpasswd.Bind(wx.EVT_SET_FOCUS, self.OnFocus)
        
    def OnSave(self, evt):
        old_password = self.tc_oldpasswd.GetValue().strip()
        if old_password == '':
            self.info.ShowMessage('原密码不能为空', flags=wx.ICON_EXCLAMATION);
            return 
        new_password = self.tc_newpasswd.GetValue().strip()
        if new_password == '':
            self.info.ShowMessage('新密码不能为空', flags=wx.ICON_WARNING);
            return 
        comfirm_password = self.tc_comfirmpasswd.GetValue().strip()
        if comfirm_password == '':
            self.info.ShowMessage('确认密码不能为空', flags=wx.ICON_WARNING);
            return
        
        if new_password != comfirm_password:
            self.info.ShowMessage('两次输入的新密码不符，请重新输入', flags=wx.ICON_WARNING);
            return
        
        result = userservice.modify_password(old_password, new_password)
        if result :
            wx.MessageBox("密码修改成功！", '温馨提示', wx.OK_DEFAULT | wx.ICON_INFORMATION)
            self.Destroy()
        else:
            self.info.ShowMessage('原密码输入错误，请重新输入', flags=wx.ICON_EXCLAMATION);
        
    def OnFocus(self, evt):
        self.info.Dismiss()
        evt.Skip()
    
    def OnCancel(self, evt):
        self.Destroy()
        
