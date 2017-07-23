from python.Application.IApplication import IApplication

class TestApp(IApplication):
    def __init__(self,rootdir,name):
        IApplication.__init__(self,rootdir,name)

    def split(self):
        self.setStatus('data')
        return {'a':None,'b':None}

    def merge(self, tasklist):
        self.log.info('App merge finished')