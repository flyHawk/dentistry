import wx

from wx.lib.mixins.treemixin import ExpansionState
import wx.lib.mixins.listctrl as listmix
import images
import wx.adv
import wx.lib.buttons as buttons
import wx.grid as gridlib
from wx.lib.intctrl import IntCtrl
import re

import db
import os
from collections import OrderedDict
from ui.catagory import AddorEditCatagoryDialog
from ui.goods import AddorEditGoodsDialog
from services import catagoryservice, goodsservice, stockservice, downloadservice
import model


class GoodsTreeCtrl(ExpansionState, wx.TreeCtrl):
    '''
    继承ExpansionState，表示记录展开状态的树状控件
    '''
    def __init__(self, parent):
        wx.TreeCtrl.__init__(self, parent, style=wx.TR_DEFAULT_STYLE | 
                             wx.TR_HAS_VARIABLE_ROW_HEIGHT)
        self.BuildTreeImageList()
        self.SetInitialSize((100, 80))

    def BuildTreeImageList(self):
        '''
        初始化树状控件图标
        '''
        imgList = wx.ImageList(16, 16)
        for png in images.catalog.values():
            imgList.Add(png.GetBitmap())

        self.AssignImageList(imgList)


class GoodsTreeCtrlPanel(wx.Panel):
    '''
    左侧物品树状控件、查询控件面板
    '''
    def __init__(self, parent):
        super(GoodsTreeCtrlPanel, self).__init__(
            parent, style=wx.TAB_TRAVERSAL | wx.CLIP_CHILDREN)

        self.filter = wx.SearchCtrl(self, style=wx.TE_PROCESS_ENTER)
        self.filter.ShowCancelButton(True)
        self.filter.Bind(wx.EVT_TEXT, self.RecreateTree)
        self.filter.Bind(wx.EVT_SEARCHCTRL_CANCEL_BTN,
                         lambda e: self.filter.SetValue(''))

        searchMenu = wx.Menu()
        searchMenu.AppendRadioItem(-1, "物品名称")
        self.filter.SetMenu(searchMenu)

        self.tree = GoodsTreeCtrl(self)
        self.tree.SetExpansionState([0, 1])
        self.tree.Bind(wx.EVT_TREE_SEL_CHANGED, self.OnSelChanged)
        self.tree.Bind(wx.EVT_RIGHT_DOWN, self.OnRightDown)
        self.tree.Bind(wx.EVT_RIGHT_UP, self.OnRightUp)

        leftBox = wx.BoxSizer(wx.VERTICAL)
        leftBox.Add(self.tree, 1, wx.EXPAND)
        leftBox.Add(wx.StaticText(self, label="查询条件:"), 0, wx.TOP | wx.LEFT, 5)
        leftBox.Add(self.filter, 0, wx.EXPAND | wx.ALL, 5)

        self.SetSizer(leftBox) 

        # 当前选中的物品
        self.currentItem = None

        # 加载数据库物品信息
        self.goods_data = self.queryGoodsData()
        self.RecreateTree()

        model.goodsListModel.addListener(self.OnUpdate)

    def queryGoodsData(self):
        goodsTree = goodsservice.get_goods_tree() 
        goods_data = OrderedDict()
        for row in goodsTree:
            catagory_name, catagory_id, goods_name, goods_id = row
            if (catagory_name, catagory_id )not in goods_data:
                if goods_name is None:
                    goods_data[catagory_name, catagory_id] = []
                else:
                    goods_data[catagory_name, catagory_id] = [(goods_name, goods_id)]
            else: 
                goods_data[catagory_name, catagory_id].append((goods_name, goods_id))
        
        return goods_data

    # ---------------------------------------------
    def OnSelChanged(self, event):
        try:
            wx.BeginBusyCursor()
            #self.GetParent().Freeze()
            item = event.GetItem()
            self.currentItem = item

            itemData = self.tree.GetItemData(item)
            #catagory_id, goods_id = itemData
            goods_id = itemData[1]
            if goods_id is None:
                return

            goodsinfo = goodsservice.get_goods(goods_id)
            model.goodsDeatilModel.set(goodsinfo)
        finally:
            wx.EndBusyCursor()
            #self.GetParent().Thaw()

    def RecreateTree(self, evt=None, current= None):

        expansionState = self.tree.GetExpansionState()

        self.tree.Freeze()
        self.tree.DeleteAllItems()
        self.root = self.tree.AddRoot("物品概览")
        self.tree.SetItemImage(self.root, 0)
        self.tree.SetItemData(self.root, (-1, None))

        treeFont = self.tree.GetFont()
        catFont = self.tree.GetFont()

        treeFont.SetWeight(wx.FONTWEIGHT_BOLD)
        catFont.SetWeight(wx.FONTWEIGHT_BOLD)
        self.tree.SetItemFont(self.root, treeFont)

        # 第一个子节点
        firstChild = None
        # 当前选中的节点
        selectedItem = None
        if current is None and self.currentItem is not None:
            current = self.tree.GetItemText(self.currentItem)
        # 过滤条件
        filterValue = self.filter.GetValue()
        count = 0

        for category, items in self.goods_data.items():
            catagory_name, catagory_id = category
            count += 1
            if filterValue:
                items = [item for item in items if filterValue.lower()
                         in item[0].lower()]
            child = self.tree.AppendItem(self.root, catagory_name, image=count)
            self.tree.SetItemFont(child, catFont)
            self.tree.SetItemData(child, (catagory_id, None))
            
            if current and current == catagory_name :
                selectedItem = child
            
            if not firstChild:
                    firstChild = child
    
            for childItem in items:
                image = count % len(images.catalog)
                goods_name, goods_id = childItem
                goodsItem = self.tree.AppendItem(child, goods_name, image=image)
                self.tree.SetItemData(goodsItem, (catagory_id, goods_id))
                
                if current and current == goods_name :
                    selectedItem = goodsItem

        self.tree.Expand(self.root)
        if selectedItem is None and firstChild:
            self.tree.Expand(firstChild)
        if filterValue:
            self.tree.ExpandAll()
        elif expansionState:
            self.tree.SetExpansionState(expansionState)
        if selectedItem:
            self.tree.SelectItem(selectedItem)

        self.tree.Thaw()
        
    def OnUpdate(self, m):
        self.goods_data = self.queryGoodsData()
        self.RecreateTree(current=m.current)
        
    def OnRightDown(self,evt):
        pt = evt.GetPosition();
        #item, flags = self.tree.HitTest(pt)
        item = self.tree.HitTest(pt)[0]
        if item:
            self.tree.SelectItem(item)
            self.currentItem = item
    
    def OnRightUp(self,evt):
        pt = evt.GetPosition();
        #item, flags = self.tree.HitTest(pt)
        item = self.tree.HitTest(pt)[0]
        if item:
            # 判断是否是root节点
            if self.tree.GetItemData(item)[0] != -1:
                menu = wx.Menu()
                item1 = menu.Append(wx.ID_ANY, "编辑")
                item2 = menu.Append(wx.ID_ANY, "删除")
                self.Bind(wx.EVT_MENU, self.OnItemEdit, item1)
                self.Bind(wx.EVT_MENU, self.OnItemDelete, item2)
                # 禁止删除非空的物品类别节点
                childrencount = self.tree.GetChildrenCount(item, recursively=True)
                if childrencount > 0:
                    menu.Enable(item2.GetId(), False)
                self.PopupMenu(menu)
                menu.Destroy()
        
    def OnItemEdit(self,evt):
        item = self.currentItem
        catagory_id, goods_id = self.tree.GetItemData(item)
        if goods_id is None:
            # 说明选择的为物品类别
            dlg = AddorEditCatagoryDialog(self.Parent.Parent, wx.ID_ANY, "修改物品类别")
            dlg.InitCtrlValue(catagory_id)
            dlg.CenterOnParent(dir=wx.BOTH)
            dlg.ShowModal()
        else:
            dlg = AddorEditGoodsDialog(self.Parent.Parent, '添加物品')
            dlg.InitCtrlValue(goods_id)
            dlg.CenterOnParent(dir=wx.BOTH)
            dlg.ShowModal()
        
    def OnItemDelete(self,evt):
        strs = "确定要删除[" + self.tree.GetItemText(self.currentItem) + "]吗?删除后将不可恢复"
        dlg = wx.MessageDialog(None, strs, '温馨提示', wx.YES_NO | wx.NO_DEFAULT | wx.CANCEL | wx.ICON_QUESTION)

        if dlg.ShowModal() in [wx.ID_NO, wx.ID_CANCEL]:
            dlg.Destroy()
            return
        dlg.Destroy()

        item = self.currentItem
        catagory_id, goods_id = self.tree.GetItemData(item)
        if goods_id is None:
            # 说明选择的为物品类别
            catagoryservice.delete_catagory(catagory_id)
        else:
            goodsservice.delete_goods(goods_id)
            
        self.tree.DeleteChildren(self.currentItem)
        self.tree.Delete(self.currentItem)
        self.currentItem = None
        self.goods_data = self.queryGoodsData()
###################################


class StockRegisterPanel(wx.Panel):
    def __init__(self, parent):
        wx.Panel.__init__(self,parent, -1, style=wx.CLIP_CHILDREN)
        
        # 计算日期选择控件的范围
        now = wx.DateTime.Today()
        beginDate = now.Subtract(wx.DateSpan(months=6))

        labelfont = wx.Font(wx.NORMAL_FONT.GetPointSize(
        ), wx.FONTFAMILY_DEFAULT, wx.FONTSTYLE_NORMAL, wx.FONTWEIGHT_BOLD)
        bs1 = wx.BoxSizer(wx.HORIZONTAL)
        label = wx.StaticText(self, wx.ID_ANY, '信息登记')
        label.SetFont(labelfont)
        bs1.Add(label, 0, wx.ALL | wx.BOTTOM, 0)

        gbSizer = wx.GridBagSizer(5, 5)
        label = wx.StaticText(self, wx.ID_ANY, '名称：')
        self.tc_goodsname = wx.TextCtrl(
            self, wx.ID_ANY, '', style=wx.TE_READONLY)
        gbSizer.Add(label, (0, 0), (1, 1), wx.ALL | wx.ALIGN_RIGHT,  5)
        gbSizer.Add(self.tc_goodsname, (0, 1), (1, 1),  wx.ALL | wx.EXPAND, 5)

        label = wx.StaticText(self, wx.ID_ANY, '物品数量：')
        self.tc_goodsNum = IntCtrl(self)
        self.tc_goodsNum.SetMin(1)
        self.tc_goodsNum.SetNoneAllowed(True)
        self.tc_goodsNum.SetValue(None)
        self.tc_goodsNum.SetMaxLength(12)
        gbSizer.Add(label, (0, 2), (1, 1),  wx.ALL | wx.ALIGN_RIGHT,  5)
        gbSizer.Add(self.tc_goodsNum, (0, 3), (1, 1), wx.ALL | wx.EXPAND, 5)

        label = wx.StaticText(self, wx.ID_ANY, '类别：')
        self.tc_goodcatagory = wx.TextCtrl(
            self, wx.ID_ANY, '', style=wx.TE_READONLY)
        gbSizer.Add(label, (1, 0), (1, 1), wx.ALL,  5)
        gbSizer.Add(self.tc_goodcatagory, (1, 1),
                    (1, 1),  wx.ALL | wx.EXPAND, 5)

        label = wx.StaticText(self, wx.ID_ANY, '经办人：')
        self.tc_op = wx.TextCtrl(self, wx.ID_ANY, '')
        self.tc_op.SetMaxLength(10)
        gbSizer.Add(label, (1, 2), (1, 1), wx.ALL | wx.ALIGN_RIGHT,  5)
        gbSizer.Add(self.tc_op, (1, 3), (1, 1), wx.ALL | wx.EXPAND, 5)

        label = wx.StaticText(self, wx.ID_ANY, '规格：')
        self.tc_goodsunit = wx.TextCtrl(
            self, wx.ID_ANY, '', )
        self.tc_goodsunit.SetMaxLength(6)
        gbSizer.Add(label, (2, 0), (1, 1), wx.ALL | wx.ALIGN_RIGHT,  5)
        gbSizer.Add(self.tc_goodsunit, (2, 1), (1, 1), wx.ALL | wx.EXPAND,  5)

        label = wx.StaticText(self, wx.ID_ANY, '经办地点：')
        self.tc_address = wx.TextCtrl(self, wx.ID_ANY, '')
        self.tc_address.SetMaxLength(24)
        gbSizer.Add(label, (2, 2), (1, 1), wx.ALL | wx.ALIGN_RIGHT,  5)
        gbSizer.Add(self.tc_address, (2, 3), (1, 1), wx.ALL | wx.EXPAND,  5)

        label = wx.StaticText(self, wx.ID_ANY, '单价：')
        self.tc_goodsprice = wx.TextCtrl(self, wx.ID_ANY, '', )
        self.tc_goodsprice.SetMaxLength(12)
        self.tc_goodsprice.Bind(wx.EVT_CHAR, self.OnFloatChar)
        gbSizer.Add(label, (3, 0), (1, 1), wx.ALL,  5)
        gbSizer.Add(self.tc_goodsprice, (3, 1), (1, 1), wx.ALL | wx.EXPAND,  5)

        label = wx.StaticText(self, wx.ID_ANY, '登记日期：')
        self.op_date = wx.adv.DatePickerCtrl(self, size=(120, -1),
                                    style=wx.adv.DP_DROPDOWN
                                    | wx.adv.DP_SHOWCENTURY)
        #self.Bind(wx.adv.EVT_DATE_CHANGED, self.OnDateChanged, self.op_date)
        self.op_date.SetRange(beginDate, wx.DateTime.Today())
        gbSizer.Add(label, (3, 2), (1, 1), wx.ALL | wx.ALIGN_RIGHT,  5)
        gbSizer.Add(self.op_date , (3, 3), (1, 1), wx.ALL | wx.EXPAND,  5)

        gbSizer.AddGrowableCol(1)
        gbSizer.AddGrowableCol(3)

        bs2 = wx.BoxSizer(wx.HORIZONTAL)
        bs2.AddStretchSpacer(1)
        b = buttons.GenButton(self, -1, "入库")
        self.Bind(wx.EVT_BUTTON, self.OnInStock, b)
        bs2.Add(b, 0, wx.ALL, 5)
        b = buttons.GenButton(self, -1, "出库")
        self.Bind(wx.EVT_BUTTON, self.OnOutStock, b)
        bs2.Add(b, 0, wx.ALL, 5)
        b = buttons.GenButton(self, -1, "重置")
        self.Bind(wx.EVT_BUTTON, self.OnReset, b)
        bs2.Add(b, 0, wx.ALL, 5)
        bs2.AddStretchSpacer(1)

        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(bs1, 0, wx.ALL, 5)
        line = wx.StaticLine(self, -1, size=(20, -1), style=wx.LI_HORIZONTAL)
        sizer.Add(line, 0, wx.GROW | wx.ALIGN_CENTER_VERTICAL |
                  wx.RIGHT | wx.TOP, 0)
        sizer.Add(gbSizer, 0, wx.ALL | wx.EXPAND, 5)
        sizer.Add(bs2, 0, wx.ALL | wx.EXPAND, 0)

        bs3 = wx.BoxSizer(wx.HORIZONTAL)
        label = wx.StaticText(self, wx.ID_ANY, '出入库记录')
        label.SetFont(labelfont)
        bs3.Add(label, 0, wx.ALL, 0)

        sizer.Add(bs3, 0, wx.ALL, 5)
        line1 = wx.StaticLine(self, -1, size=(20, -1), style=wx.LI_HORIZONTAL)
        sizer.Add(line1, 0, wx.GROW | wx.ALIGN_CENTER_VERTICAL |
                  wx.RIGHT | wx.TOP, 0)
        
        bs4 = wx.BoxSizer(wx.HORIZONTAL)
        label = wx.StaticText(self, wx.ID_ANY, '物品名称：')
        self.s_goodsname = wx.TextCtrl(self, wx.ID_ANY, size=(120,-1), style=wx.TE_PROCESS_ENTER)
        self.s_goodsname.SetMaxLength(16)
        self.Bind(wx.EVT_TEXT_ENTER, self.OnTextEnter, self.s_goodsname)
        
        bs4.Add(label, 0, wx.ALL | wx.ALIGN_RIGHT|wx.ALIGN_CENTER_VERTICAL,  5)
        bs4.Add(self.s_goodsname,0, wx.ALL | wx.EXPAND, 5)
        
        label = wx.StaticText(self, wx.ID_ANY, '登记日期：')
        self.registerDate = wx.adv.DatePickerCtrl(self, size=(120,-1), style=wx.adv.DP_DROPDOWN| wx.adv.DP_SHOWCENTURY)
        self.registerDate.SetRange(wx.DateTime.Today().Subtract(wx.DateSpan(months=6)), wx.DateTime.Today())
        self.Bind(wx.adv.EVT_DATE_CHANGED, self.OnDateChanged, self.registerDate)
        
        bs4.Add(label, 0, wx.ALL | wx.ALIGN_RIGHT|wx.ALIGN_CENTER_VERTICAL,  5)
        bs4.Add(self.registerDate, 0, wx.ALL | wx.EXPAND, 5)
        cBtn = wx.ContextHelpButton(self)
        cBtn.SetHelpText("默认显示当天登记的记录，并且可以对记录进行删除和修改操作。")
        bs4.Add(cBtn, 0, wx.ALL | wx.EXPAND, 5)
        
        bs5 = wx.BoxSizer(wx.VERTICAL)
        self.list = SockListCtrl(self, -1, style=wx.LC_REPORT | wx.LC_SINGLE_SEL| wx.BORDER_NONE| wx.LC_HRULES | wx.LC_VRULES)
        self.list.Bind(wx.EVT_COMMAND_RIGHT_CLICK, self.OnRightClick)
        #self.Bind(wx.EVT_LIST_ITEM_ACTIVATED, self.OnItemActivated, self.list)
        #self.Bind(wx.EVT_LIST_ITEM_SELECTED, self.OnItemSelected, self.list)
        
        bs5.Add(self.list, 1, wx.ALL | wx.EXPAND, 5)

        sizer.Add(bs4, 0, wx.ALL | wx.EXPAND, 5)
        sizer.Add(bs5, 1, wx.ALL|wx.EXPAND, 5)
        self.SetSizer(sizer)

        
        self.goodsId = None
        self.currentItem = None
        
        model.goodsDeatilModel.addListener(self.OnUpdate)
        self.queryStocksByDate()
    
    def queryStocksByDate(self):
        registerDate = self.registerDate.GetValue().Format('%Y-%m-%d')
        goodsName = self.s_goodsname.GetValue().strip()
        result = stockservice.get_stocks({
                'goods_name': goodsName,
                'startdate': registerDate
            });
        self.setData(result)
    
    def OnDateChanged(self, evt):
        self.queryStocksByDate()
        
    def OnTextEnter(self,evt):
        self.queryStocksByDate()
        
    def OnRightClick(self, evt):
        # 只第一次绑定事件
        if not hasattr(self, "removeId"):
            self.removeId = wx.NewIdRef()
            self.editId = wx.NewIdRef()

            self.Bind(wx.EVT_MENU, self.OnDelete, id=self.removeId)
            #self.Bind(wx.EVT_MENU, self.OnEdit, id=self.editId)
           
        menu = wx.Menu()
        menu.Append(self.removeId, "删除")
        #menu.Append(self.editId, "编辑")
        self.PopupMenu(menu)
        menu.Destroy()
    
#     def OnEdit(self, evt):
#         if self.currentItem is not None:
#             self.list.EditLabel(self.currentItem)
#         else:
#             wx.MessageBox('请选择一条记录', '温馨提示', wx.OK_DEFAULT|wx.ICON_WARNING)
#         
#     def OnItemSelected(self, event):
#         self.list.OpenEditor(5, event.Index)

    def OnUpdate(self, m):
        self.tc_goodsname.SetValue(m.data['GOODS_NAME'])
        self.tc_goodsunit.SetValue(m.data['GOODS_UNIT'])
        self.tc_goodsprice.SetValue(str(m.data['GOODS_PRICE']))
        self.tc_goodcatagory.SetValue(m.data['CATAGORY_NAME'])
        self.goodsId = m.data['ID']

    def OnInStock(self, evt):
        self.SaveStock(1)
        
    def OnOutStock(self, evt):
        self.SaveStock(0)

    def SaveStock(self, stock_type):
        if self.goodsId is None:
            wx.MessageBox('请在左侧选择要登记的物品!', '温馨提示', wx.YES_DEFAULT | wx.ICON_EXCLAMATION)
            return

        # 输入值校验
        goodnum = self.tc_goodsNum.GetValue()
        if goodnum is None :
            wx.MessageBox('请输入物品数量!', '温馨提示', wx.YES_DEFAULT | wx.ICON_EXCLAMATION)
            self.tc_goodsNum.SetBackgroundColour("pink")
            self.tc_goodsNum.SetFocus()
            self.tc_goodsNum.Refresh()
            return
        elif goodnum < 1:
            wx.MessageBox('物品数量至少为1', '温馨提示', wx.YES_DEFAULT | wx.ICON_EXCLAMATION)
            self.tc_goodsNum.SetBackgroundColour("pink")
            self.tc_goodsNum.SetFocus()
            self.tc_goodsNum.Refresh()
            return
        else:
            self.tc_goodsNum.SetBackgroundColour(
                wx.SystemSettings.GetColour(wx.SYS_COLOUR_WINDOW))
            self.tc_goodsNum.Refresh()
            
        goodunit = self.tc_goodsunit.GetValue().strip()
        if goodunit is None or goodunit  == '':
            wx.MessageBox('请输入物品规格!', '温馨提示', wx.YES_DEFAULT | wx.ICON_EXCLAMATION)
            self.tc_goodsunit.SetBackgroundColour("pink")
            self.tc_goodsunit.SetFocus()
            self.tc_goodsunit.Refresh()
            return
        else:
            self.tc_goodsunit.SetBackgroundColour(
                wx.SystemSettings.GetColour(wx.SYS_COLOUR_WINDOW))
            self.tc_goodsunit.Refresh()

        stock_date = self.op_date.GetValue().Format('%Y-%m-%d')
        goods_id = self.goodsId
        goodunit = self.tc_goodsunit.GetValue().strip()
        goodprice = self.tc_goodsprice.GetValue().strip()
        if goodprice !='':
            pattern= re.compile('^[0-9]*\.?[0-9]{,2}$')
            if re.match(pattern,goodprice ) is None:
                wx.MessageBox('单价不符合要求的格式', '温馨提示', wx.YES_DEFAULT | wx.ICON_EXCLAMATION)
                return
        
        goods_num = self.tc_goodsNum.GetValue()
        op_person = self.tc_op.GetValue().strip()
        op_area = self.tc_address.GetValue().strip()
        
        params = {
            'STOCK_TYPE': stock_type,
            'STOCK_DATE': stock_date,
            'GOODS_ID': goods_id,
            'GOODS_NUM': goods_num,
            'GOODS_UNIT': goodunit,
            'GOODS_PRICE': goodprice,
            'OP_PERSON': op_person, 
            'OP_AREA': op_area
            }
        
        stockservice.add_stock(params)
        wx.MessageBox('物品登记成功!', '温馨提示', wx.OK|wx.ICON_INFORMATION)
        self.queryStocksByDate()

    def OnDelete(self, event):
        count = self.list.SelectedItemCount
        if count < 1:
            dial = wx.MessageDialog(self, '请选择要删除的记录!', '温馨提示', wx.OK)
            dial.ShowModal()
            dial.Destroy()
        else:
            dlg = wx.MessageDialog(None, u"确定要删除该条记录吗？删除将不可恢复", u"温馨提示", wx.YES_NO | wx.ICON_QUESTION)
            if dlg.ShowModal() == wx.ID_YES:
                index = self.list.GetFirstSelected()
                item = self.list.GetItem(index)
                stockId = item.GetData()

                result = stockservice.delete_stock(stockId)
                if result > 0:
                    self.list.DeleteItem(index)
                    dial = wx.MessageDialog(self, '记录已经成功删除!', '温馨提示', wx.OK)
                    dial.ShowModal()
                    dial.Destroy()
            dlg.Destroy()


    def setData(self, data):
        self.list.DeleteAllItems()
        self.list.data = data
        for idx , row in enumerate(data):
            index = self.list.InsertItem(self.list.GetItemCount(), str(idx+1))
            for col, text in enumerate(row[1:]):
                if not text:
                    text = ''
                if col == 0:
                    text = '入库' if text == 1 else '出库'
                self.list.SetItem(index, col+1, str(text))
            self.list.SetItemData(index, row[0]) 

        self.list.SetColumnWidth(2, wx.LIST_AUTOSIZE)

    def OnFloatChar(self, evt):
        key = evt.GetKeyCode()
        if key < wx.WXK_SPACE or key == wx.WXK_DELETE or key > 255 or key==46:
            evt.Skip()
            return
        if chr(key).isdigit():
            evt.Skip()
            return
        return
    
    def OnReset(self,evt):
        self.goodsId = None
        self.tc_goodsname.SetValue('')
        self.tc_goodsunit.SetValue('')
        self.tc_goodsprice.SetValue('')
        self.tc_goodcatagory.SetValue('')
        self.tc_goodsNum.SetValue(None)
        self.tc_op.SetValue('')
        self.tc_address.SetValue('')
        self.op_date.SetValue(wx.DateTime.Today())
        
    
class StockReportPanel(wx.Panel):
    def __init__(self, parent):
        super(StockReportPanel, self).__init__(parent, wx.ID_ANY, style=wx.CLIP_CHILDREN)
        
        self.InitUI()
        
    def InitUI(self):
        
        
        labelfont = wx.Font(wx.NORMAL_FONT.GetPointSize(), wx.FONTFAMILY_DEFAULT, wx.FONTSTYLE_NORMAL, wx.FONTWEIGHT_BOLD)
        title_label = wx.StaticText(self, wx.ID_ANY, '查询条件')
        title_label.SetFont(labelfont)
        
        line1 = wx.StaticLine(self, wx.ID_ANY,size=(20,-1),style=wx.LI_HORIZONTAL)
        
        self.inStock_cb = wx.CheckBox(self, wx.ID_ANY, "入库", style=wx.ALIGN_RIGHT)
        self.outStock_cb = wx.CheckBox(self, wx.ID_ANY, "出库",style=wx.ALIGN_RIGHT)
        self.inStock_cb.SetValue(True)
        self.outStock_cb.SetValue(True)
        
        goodsname_label = wx.StaticText(self, wx.ID_ANY, '物品名称：')
        date_label = wx.StaticText(self, wx.ID_ANY, '起止日期：')
        concat_label = wx.StaticText(self, wx.ID_ANY, '至')
         
        self.goodsname_tc = wx.TextCtrl(self, wx.ID_ANY, size=(120,-1))
        self.goodsname_tc.SetMaxLength(16)
         
        self.startdate = wx.adv.DatePickerCtrl(self, size=(120,-1), style=wx.adv.DP_DROPDOWN| wx.adv.DP_SHOWCENTURY)
        self.startdate.SetRange(wx.DateTime.Today().Subtract(wx.DateSpan(months=6)), wx.DateTime.Today())
        self.startdate.SetValue(wx.DateTime.Today().Subtract(wx.DateSpan(days=6)))
                                
        self.endate = wx.adv.DatePickerCtrl(self, size=(120,-1),style=wx.adv.DP_DROPDOWN| wx.adv.DP_SHOWCENTURY)
        self.endate.SetRange(wx.DateTime.Today().Subtract(wx.DateSpan(months=6)), wx.DateTime.Today())
         
        gbSizer = wx.GridBagSizer(5, 5)
        gbSizer.Add(self.inStock_cb, (0,0), (1,1), wx.ALL, 5)
        gbSizer.Add(self.outStock_cb, (0,1), (1,1),wx.ALL, 5)
        gbSizer.Add(goodsname_label, (1,0), (1,1),wx.ALL|wx.ALIGN_CENTER_VERTICAL, 5)
        gbSizer.Add(self.goodsname_tc, (1,1), (1,1),wx.ALL, 5)
        gbSizer.Add(date_label, (2,0), (1,1),wx.ALL|wx.ALIGN_CENTER_VERTICAL, 5)
        gbSizer.Add(self.startdate, (2,1), (1,1),wx.ALL, 5)
        gbSizer.Add(concat_label, (2,2), (1,1),wx.ALL|wx.ALIGN_CENTER, 5)
        gbSizer.Add(self.endate, (2,3), (1,1),wx.ALL, 5)
        
        hBox_btn = wx.BoxSizer(wx.HORIZONTAL)
        hBox_btn.AddStretchSpacer()
        b = buttons.GenButton(self, -1, "查询")
        hBox_btn.Add(b, 0, wx.ALL, 5)
        self.Bind(wx.EVT_BUTTON, self.OnSearch, b)
        b = buttons.GenButton(self, -1, "导出EXCEL")
        self.Bind(wx.EVT_BUTTON, self.OnDownload, b)
        hBox_btn.Add(b, 0, wx.ALL, 5)
        hBox_btn.AddStretchSpacer()
        
        #grid
        self.grid = gridlib.Grid(self)
        table = StockDataTable([])
        self.grid.SetTable(table, True)
        self.grid.SetMargins(0,0)
        self.grid.AutoSizeColumns(True)
        
        vBox = wx.BoxSizer(wx.VERTICAL)
        vBox.Add(title_label, 0, wx.ALL, 5 )
        vBox.Add(line1, 0, wx.GROW | wx.ALIGN_CENTER_VERTICAL |
                  wx.RIGHT | wx.TOP, 0)
        vBox.Add(gbSizer, 0, wx.ALL, 5 )
        vBox.Add(hBox_btn, 0, wx.ALL|wx.EXPAND, 5 )
        vBox.Add(self.grid, 1, wx.GROW|wx.ALL, 5)
        
        self.SetSizer(vBox)
        
    def OnSearch(self, evt):
        goodsname = self.goodsname_tc.GetValue().strip()
        startdate = self.startdate.GetValue().Format('%Y-%m-%d')
        enddate = self.endate.GetValue().Format('%Y-%m-%d')

        params = {
                'goods_name':  '%' +goodsname+'%',
                'startdate': startdate,
                'enddate': enddate,
                'instock': self.inStock_cb.IsChecked(),
                'outstock': self.outStock_cb.IsChecked()
            }
        stocks = stockservice.get_stocks(params)
        
        table = StockDataTable(stocks)
        self.grid.SetTable(table, True)

        self.grid.SetMargins(0,0)
        self.grid.AutoSizeColumns(True)
        self.grid.Refresh()
        
    def OnDownload(self, evt):
        goodsname = self.goodsname_tc.GetValue().strip()
        startdate = self.startdate.GetValue().Format('%Y%m%d')
        enddate = self.endate.GetValue().Format('%Y%m%d')
        
        params = {
            'goods_name':  '%' +goodsname+'%',
            'startdate': self.startdate.GetValue().Format('%Y-%m-%d') ,
            'enddate': self.endate.GetValue().Format('%Y-%m-%d'),
            'instock': self.inStock_cb.IsChecked(),
            'outstock': self.outStock_cb.IsChecked()
        }
        data = stockservice.get_stocks_dict(params)
        if not data:
            wx.MessageBox("未查询到满足条件的结果，无法下载","温馨提示",wx.OK_DEFAULT|wx.ICON_WARNING)
            return 
        
        if self.inStock_cb.IsChecked() and not self.outStock_cb.IsChecked():
            filename_prefix = startdate + '至' + enddate + '入库明细表'
        elif not self.outStock_cb.IsChecked() and self.outStock_cb.IsChecked():
            filename_prefix = startdate + '至' + enddate + '出库明细表'
        else:
            filename_prefix = startdate + '至' + enddate + '出入库明细表'
            
        defaultFilename = filename_prefix + '.xlsx'
        wildcard = "Excel工作簿 (*.xlsx)|*.xlsx|All files (*.*)|*.*"
        dlg = wx.FileDialog(
                    self, message="保存为 ...", defaultDir=os.getcwd(),
                    defaultFile=defaultFilename, wildcard=wildcard, style=wx.FD_SAVE | wx.FD_OVERWRITE_PROMPT
                    )
        dlg.SetFilterIndex(0)

        if dlg.ShowModal() == wx.ID_OK:
            try:
                self.Freeze()
                wx.BeginBusyCursor()
                path = dlg.GetPath()
        
                
                if self.inStock_cb.IsChecked() and not self.outStock_cb.IsChecked():
                    title = startdate + '至' + enddate + '入库明细表'
                    downloadservice.downloadInStockDetail(path, title, data)
                elif not self.outStock_cb.IsChecked() and self.outStock_cb.IsChecked():
                    title = startdate + '至' + enddate + '出库明细表'
                    downloadservice.downloadOutStockDetail(path, title, data)
                else:
                    title = startdate + '至' + enddate + '出入库明细表'
                    downloadservice.downloadAllStockDetail(path, title, data)
                    
            finally:
                self.Thaw()
                wx.EndBusyCursor()
        
class StockDataTable(gridlib.GridTableBase):
    
    def __init__(self, stockdata):
        super(StockDataTable, self).__init__()
        
        self.fieldDict = {
                '1' : '入库',
                '0': '出库'
            }
        
        self.colLabels = ['出库/入库', '物品类别', '物品名称', '登记日期',
                          '数量', '规格', '价格', '经办人','科室/地点']
        
        self.default_number_rows = 0
        self.default_number_cols = len(self.colLabels)
        self.data = stockdata
        
        self.odd=gridlib.GridCellAttr()
        self.odd.SetBackgroundColour("light blue")
        #self.odd.SetFont(wx.Font(10, wx.SWISS, wx.NORMAL, wx.BOLD))
        self.even=gridlib.GridCellAttr()
        self.even.SetBackgroundColour('Seashell')
        #self.even.SetFont(wx.Font(10, wx.SWISS, wx.NORMAL, wx.BOLD))

    def GetAttr(self, row, col, kind):
        attr = [self.even, self.odd][row % 2]
        attr.IncRef()
        return attr
    
    def GetColLabelValue(self, col):
        return self.colLabels[col]
    
    def GetNumberRows(self):
        
        if self.data :
            return len(self.data) + 1
        else:
            return self.default_number_rows

    def GetNumberCols(self):
        if self.data :
            return len(self.data[0])-1
        else:
            return self.default_number_cols

    def IsEmptyCell(self, row, col):
        try:
            return not self.data[row][col+1]
        except IndexError:
            return True
        
    def GetValue(self, row, col):
        try:
            if col==0:
                return self.fieldDict[str(self.data[row][col+1])]
            return self.data[row][col+1]
        except IndexError:
            return ''
        
    def SetValue(self, row, col, value):
        pass


class SockListCtrl(wx.ListCtrl,
                   listmix.ListCtrlAutoWidthMixin,
                   listmix.TextEditMixin):

    def __init__(self, parent, ID, pos=wx.DefaultPosition,
                 size=wx.DefaultSize, style=0):
        super(SockListCtrl, self).__init__( parent, ID, pos, size, style)


        self.columns_labels = ['序号','类型', '物品类别','物品名称','登记日期','数量','规格','价格','经办人','经办地点']
        listmix.ListCtrlAutoWidthMixin.__init__(self)
        self.Populate()
        listmix.TextEditMixin.__init__(self)

        self.data = None
        self.oldvalue = None

    def Populate(self):
        
        for col, text in enumerate(self.columns_labels ):
            self.InsertColumn(col, text)

        
    def OpenEditor(self, col, row):
        if col in [5,8,9]:
            
            listmix.TextEditMixin.OpenEditor(self, col, row)
            item = self.GetItem(self.curRow,self.curCol)
            self.oldvalue = item.GetText()
        else:
            pass

    def CloseEditor(self, evt=None):
        listmix.TextEditMixin.CloseEditor(self, evt)
        item = self.GetItem(self.curRow,self.curCol)
        newValue = item.GetText()
        oldValue = self.oldvalue
        if self.curCol == 5 :
            try:
                newValue = int(newValue)
            except ValueError:
                dial = wx.MessageDialog(self, '填写的必须是数字!', '温馨提示', wx.OK)
                dial.ShowModal()
                dial.Destroy()
                item.SetText(str(oldValue))
                self.SetItem(item)
                self.RefreshItem(self.curRow)
                return
        if str(newValue).strip() != str(oldValue).strip() :
            # 修改成功
            msg = '您将' + self.columns_labels[self.curCol] + '由[' + str(oldValue) + ']修改为[' + str(newValue) +']，确定要修改吗？'
            dlg = wx.MessageDialog(self, msg, u"温馨提示", wx.YES_NO | wx.ICON_QUESTION)
            if dlg.ShowModal() == wx.ID_YES:
                stockId = item.GetData()
                if self.curCol == 5:
                    sql = 'update stock set goods_num = ? where id = ? '
                elif self.curCol == 8:
                    sql = 'update stock set op_person = ? where id = ? '
                elif self.curCol == 9:
                    sql = 'update stock set op_area = ? where id = ? '
                params = ( newValue, stockId)
                db.update(sql, params)
            else:  
                item.SetText(str(oldValue))
                self.SetItem(item)
                self.RefreshItem(self.curRow)
            dlg.Destroy()
