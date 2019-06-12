import wx
import wx.adv
import images

class CustomTaskBarIcon(wx.adv.TaskBarIcon):
    
    TBMENU_RESTORE = wx.NewIdRef()
    TBMENU_CLOSE   = wx.NewIdRef()

    def __init__(self, frame):
        super(CustomTaskBarIcon, self).__init__(wx.adv.TBI_DOCK)
        self.frame = frame

        icon = self.MakeIcon(images.logo.GetImage())
        self.SetIcon(icon,'牙科物品管理系统')
        
        self.Bind(wx.adv.EVT_TASKBAR_LEFT_DCLICK, self.OnTaskBarActivate)
        self.Bind(wx.EVT_MENU, self.OnTaskBarActivate, id=self.TBMENU_RESTORE)
        self.Bind(wx.EVT_MENU, self.OnTaskBarClose, id=self.TBMENU_CLOSE)
        
    def MakeIcon(self, img):
        '''
        根据不同平台调整图标大小
        '''
        if "wxMSW" in wx.PlatformInfo:
            img = img.Scale(16, 16)
        elif "wxGTK" in wx.PlatformInfo:
            img = img.Scale(22, 22)
        
        icon = wx.Icon(img.ConvertToBitmap())
        return icon   

    def CreatePopupMenu(self):
        '''
        当捕获到EVT_RIGHT_DOWN事件时，TaskBarIcon类会自动调用该方法弹出菜单
        '''
        menu = wx.Menu()
        menu.Append(self.TBMENU_RESTORE, "打开主界面")
        menu.Append(self.TBMENU_CLOSE,   "退出")
        return menu
    
    def OnTaskBarActivate(self, evt):
        if self.frame.IsIconized():
            self.frame.Iconize(False)
        if not self.frame.IsShown():
            self.frame.Show(True)
        self.frame.Raise()
    
    def OnTaskBarClose(self, evt):
        wx.CallAfter(self.frame.Close)