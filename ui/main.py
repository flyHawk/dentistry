import wx
import wx.aui as aui
import images
import sys
import traceback
import wx.lib.agw.genericmessagedialog as GMD

from wx.lib.stattext import GenStaticText as StaticText
from wx.lib.statbmp  import GenStaticBitmap as StaticBitmap

from ui.setting import ResetLoginPasswdDialog
from ui.goods import AddorEditGoodsDialog
from ui.catagory import AddorEditCatagoryDialog
from ui.stock import GoodsTreeCtrlPanel, StockRegisterPanel, StockReportPanel
from ui.taskbar import CustomTaskBarIcon
from services import userservice


class MainFrame(wx.Frame):

    def __init__(self):
        super(MainFrame, self).__init__(None, wx.ID_ANY,
                                          title='牙科物品出入库管理系统', size=(1200, 800),
                                          style=wx.DEFAULT_FRAME_STYLE)
        sys.excepthook = MyExceptionHook
        # 工具栏
        self.MakeToolBar()
        # 状态栏
        self.CreateStatusBar()
        self.SetStatusText("欢迎使用!")
        # 布局
        self.allowAuiFloating = False
        self.mgr = None
        self.InitUI()
        # 位置和图标
        self.Centre( wx.BOTH )
        self.SetIcon(images.logo.GetIcon()) 
        # 任务栏图标
        self.tbIcon = CustomTaskBarIcon(self)
        
        self.Bind(wx.EVT_CLOSE, self.OnCloseWindow)
        self.Bind(wx.EVT_ICONIZE, self.onMinimize)
       
    def MakeToolBar(self):
        TBFLAGS = ( wx.TB_HORIZONTAL
            | wx.NO_BORDER
            | wx.TB_FLAT
            )
        tb = self.CreateToolBar( TBFLAGS )
        tsize = (24,24)
        catagory_new_bmp =  images.catagory_new.GetBitmap()
        goods_new_bmp = images.goods_new.GetBitmap()
        tb.SetToolBitmapSize(tsize)

        tb.AddTool(10, "新增类别", catagory_new_bmp, wx.NullBitmap, wx.ITEM_NORMAL, "新增类别", "增加物品类别，用于物品归类，便于物品管理'", None)
        tb.AddTool(20, "新增物品", goods_new_bmp, wx.NullBitmap, wx.ITEM_NORMAL, "新增物品", "添加新的物品", None)
        self.Bind(wx.EVT_TOOL, self.OnAddCatagory, id=10)
        self.Bind(wx.EVT_TOOL, self.OnAddGoods, id=20)

        tb.AddStretchableSpace()
        login_msg = '您好，{}，欢迎使用系统! '.format(userservice.get_current_user().get('username'))
        login_info = wx.StaticText(tb, wx.ID_ANY, login_msg)
        tb.AddControl(login_info)
        tb.AddSeparator()
        
        bmp = StaticBitmap(tb, -1, images.setting.GetBitmap()) 
        tb.AddControl(bmp)
        modifypass_lb = StaticText(tb, -1, "修改密码")
        tb.AddControl(modifypass_lb)
        modifypass_lb.Bind(wx.EVT_MOUSE_EVENTS, self.OnModifyPassword)
        
        tb.Realize()
    
    def InitUI (self):
       
        panel = wx.Panel(self)

        self.mgr = aui.AuiManager()
        self.mgr.SetManagedWindow(panel)
        
        notebook = wx.Notebook(panel, -1, style=wx.CLIP_CHILDREN)
        stockRegisterPanel = StockRegisterPanel(notebook )
        stockReportPanel = StockReportPanel(notebook)
        notebook.AddPage(stockRegisterPanel, '物品登记')
        notebook.AddPage(stockReportPanel, '出入库报表')
        notebook.SetSelection(0)
        
        goodsTreeCtrlPanel = GoodsTreeCtrlPanel(panel)
                
        self.mgr.AddPane(notebook, aui.AuiPaneInfo().CenterPane().Name("Notebook"))
        self.mgr.AddPane(goodsTreeCtrlPanel,
                         aui.AuiPaneInfo().
                         Left().Layer(2).BestSize((240, -1)).
                         MinSize((240, -1)).
                         Floatable(self.allowAuiFloating).FloatingSize((240, 700)).
                         Caption("").
                         CloseButton(False).
                         Name("GoodsTree"))
        
        self.mgr.Update()
        bSizer = wx.BoxSizer( wx.VERTICAL )
        bSizer.Add(panel, 1, wx.ALL|wx.EXPAND, 0)
        self.SetSizer( bSizer  )
        
        self.Layout()
    
    def OnAddCatagory(self, event):
        dlg = AddorEditCatagoryDialog(self, wx.ID_ANY, "添加物品类别")
        dlg.CenterOnParent()
        dlg.ShowModal()
        
    def OnAddGoods(self, event):
        goodsDialog = AddorEditGoodsDialog(self, '添加物品')
        goodsDialog.CenterOnParent()
        goodsDialog.ShowModal()
    
    def OnModifyPassword(self,evt):
        win = evt.GetEventObject()
        if evt.Moving():
            # 设置手表指针和文本下划线
            win.SetCursor(wx.Cursor(wx.CURSOR_HAND))
            font = win.GetFont()
            font.SetUnderlined(True)
            win.SetFont(font)
 
        elif evt.LeftUp():
            dlg = ResetLoginPasswdDialog(self, -1, "登录密码修改")
            dlg.CenterOnParent()
            dlg.ShowModal()
            # 解决取消对话框时，文本显示为灰色的问题
            win.Enable()
            
        else:
            win.SetCursor(wx.NullCursor)
            font = win.GetFont()
            font.SetUnderlined(False)
            win.SetFont(font)
            
        evt.Skip()
    
    def OnCloseWindow(self,evt):
        if self.mgr:
            self.mgr.UnInit()
        self.tbIcon.RemoveIcon()
        self.tbIcon.Destroy()
        self.Destroy()
        
    def onMinimize(self, evt):
        if self.IsIconized():
            self.Hide()

def MyExceptionHook(etype, value, trace):
    """
    Handler for all unhandled exceptions.

    :param `etype`: the exception type (`SyntaxError`, `ZeroDivisionError`, etc...);
    :type `etype`: `Exception`
    :param string `value`: the exception error message;
    :param string `trace`: the traceback header, if any (otherwise, it prints the
     standard Python header: ``Traceback (most recent call last)``.
    """
    frame = wx.GetApp().GetTopWindow()
    tmp = traceback.format_exception(etype, value, trace)
    exception = "".join(tmp)

    dlg = ExceptionDialog(exception)
    dlg.ShowModal()
    dlg.Destroy()
    
class ExceptionDialog(GMD.GenericMessageDialog):
    """
    The dialog to show an exception
    """

    def __init__(self, msg):
        """Constructor"""
        GMD.GenericMessageDialog.__init__(self, None, msg, "Exception!",
                                          wx.OK|wx.ICON_ERROR)