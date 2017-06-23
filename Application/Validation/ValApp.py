from python.Application.IApplication import IApplication
import os

class ValApp(IApplication):
    def __init__(self, rootdir, name):
        IApplication.__init__(self, rootdir, name)
        self.topdir=None
        self.anawork=[]

    def split(self):
        # 根据类型， 目录拆分，保有 ref的文件目录路径
        pass