'''
-------------------system module-------------------
'''
import wx

'''
-------------------user module---------------------
'''
from ui.login import LoginDialog

provider = wx.SimpleHelpProvider()
wx.HelpProvider.Set(provider)

if __name__ == '__main__':
        app = wx.App()
        
        loginDlg = LoginDialog()
        loginDlg.ShowModal()
        loginDlg.Destroy()
        
        app.MainLoop()
