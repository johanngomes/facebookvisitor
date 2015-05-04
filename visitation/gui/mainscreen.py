__author__ = 'johann'

import wx


class MainScreen(wx.Panel):
    def __init__(self, parent):
        wx.Panel.__init__(self, parent, size=(250, 150))

        self.static_text_1 = wx.StaticText(self, label="Discovered Actors :", pos=(20, 120))
        self.static_text_2 = wx.StaticText(self, label="Discovered Posts :", pos=(20, 150))
        self.static_text_3 = wx.StaticText(self, label="Discovered Comments :", pos=(20, 180))

        self.static_text_4 = wx.StaticText(self, label="page", pos=(220, 85))
        self.static_text_5 = wx.StaticText(self, label="user", pos=(300, 85))
        self.static_text_6 = wx.StaticText(self, label="group", pos=(380, 85))
        self.static_text_7 = wx.StaticText(self, label="event", pos=(460, 85))

        self.static_text_8 = wx.StaticText(self, label="Total visits:", pos=(20, 240))
        self.static_text_9 = wx.StaticText(self, label="In-time visits:", pos=(20, 270))

        self.static_text_10 = wx.StaticText(self, label="Execution time:", pos=(20, 330))
        self.static_text_11 = wx.StaticText(self, label="Accumulated time:", pos=(20, 360))

        # A multiline TextCtrl - This is here to show how the events work in this program,
        # don't pay too much attention to it
        # self.logger = wx.TextCtrl(self, pos=(300, 20), size=(200, 300), style=wx.TE_MULTILINE | wx.TE_READONLY)

        # A button
        # self.button =wx.Button(self, label="Save", pos=(200, 325))
        # self.Bind(wx.EVT_BUTTON, self.OnClick, self.button)

        # the edit control - one line version.
        # self.lblname = wx.StaticText(self, label="Your name :", pos=(20, 60))
        # self.editname = wx.TextCtrl(self, value="Enter here your name", pos=(150, 60), size=(140, -1))
        # self.Bind(wx.EVT_TEXT, self.EvtText, self.editname)
        # self.Bind(wx.EVT_CHAR, self.EvtChar, self.editname)

        # the combobox Control
        # self.sampleList = ['friends', 'advertising', 'web search', 'Yellow Pages']
        # self.lblhear = wx.StaticText(self, label="How did you hear from us ?", pos=(20, 90))
        # self.edithear = wx.ComboBox(self, pos=(150, 90), size=(95, -1), choices=self.sampleList, style=wx.CB_DROPDOWN)
        # self.Bind(wx.EVT_COMBOBOX, self.EvtComboBox, self.edithear)
        # self.Bind(wx.EVT_TEXT, self.EvtText, self.edithear)

        # Checkbox
        # self.insure = wx.CheckBox(self, label="Do you want Insured Shipment ?", pos=(20, 180))
        # self.Bind(wx.EVT_CHECKBOX, self.EvtCheckBox, self.insure)

        # Radio Boxes
        # radioList = ['blue', 'red', 'yellow', 'orange', 'green', 'purple', 'navy blue', 'black', 'gray']
        # rb = wx.RadioBox(self, label="What color would you like ?", pos=(20, 210), choices=radioList,  majorDimension=3,
        #                  style=wx.RA_SPECIFY_COLS)
        # self.Bind(wx.EVT_RADIOBOX, self.EvtRadioBox, rb)

    def EvtRadioBox(self, event):
        self.logger.AppendText('EvtRadioBox: %d\n' % event.GetInt())
    def EvtComboBox(self, event):
        self.logger.AppendText('EvtComboBox: %s\n' % event.GetString())
    def OnClick(self,event):
        self.logger.AppendText(" Click on object with Id %d\n" % event.GetId())
    def EvtText(self, event):
        self.logger.AppendText('EvtText: %s\n' % event.GetString())
    def EvtChar(self, event):
        self.logger.AppendText('EvtChar: %d\n' % event.GetKeyCode())
        event.Skip()
    def EvtCheckBox(self, event):
        self.logger.AppendText('EvtCheckBox: %d\n' % event.Checked())


app = wx.App(False)
frame = wx.Frame(None, wx.ID_ANY, "Actor Network Monitor", size=(600, 400))
panel = MainScreen(frame)
frame.Show()
app.MainLoop()