import datetime
import types
import os, sys
import select
import subprocess
from Parser import Parser


class status:
    (SUCCESS, FAIL, TIMEOUT, OVERFLOW, ANR) = range(0, 5)
    DES = {
        FAIL: 'Return code is not zero',
        TIMEOUT: 'Run time exceeded',
        OVERFLOW: 'Memory overflow',
        ANR: 'Not responding'
    }

    @staticmethod
    def describe(stat):
        if status.DES.has_key(stat):
            return status.DES[stat]

class Process:
    def __init__(self, exe, logfile, shell=True ,timeout=0):
        self.shell = shell

        self.executable = exe

        # Log file name depends on what we are running
        self.logFile = logfile
        # Merge stdout and stderr
        self.stdout = self.stderr = subprocess.PIPE
        self.process = None
        self.pid = None
        self.returncode = None
        self.status = None
        self.timeout = timeout
        self.duration = None
        self.start = None
        self.killed = None
        self.fatalLine = None

    def run(self):
        self.start = datetime.datetime.now()
        self.process = subprocess.Popen(args=self.executable, shell=self.shell, stdout=self.stdout,
                                        stderr=subprocess.STDOUT)
        self.pid = self.process.pid
        logFile = open(self.logFile, 'w+')
        while True:
            fs = select.select([self.process.stdout], [], [])
            if not fs[0]:
                # No response
                self.status = status.ANR
                self._kill()
                break
            if self.process.stdout in fs[0]:
                # Incoming message to parse
                data = os.read(self.process.stdout.fileno(), 1024)
                if not data:
                    break
                # If it is called in analysis step, we print the log info to screen
                logFile.write(data)
        self._burnProcess()
        logFile.close()

    def getDuration(self):
        return self.duration

    def _kill(self):
        if not self.process:
            return
        import os, signal
        try:
            os.kill(self.pid, signal.SIGKILL)
            os.waitpid(-1, os.WNOHANG)
        except:
            pass

    def _burnProcess(self):
        self.returncode = self.process.wait()
        if self.status:
            return
        if 0 == self.returncode:
            self.status = status.SUCCESS
        else:
            self.status = status.FAIL
        # FIXME: it seems that root macro process won't give a 0 return code
        if type(self.executable) == types.ListType and self.executable[0] == 'root':
            self.status = status.SUCCESS
        if type(self.executable) == types.StringType and self.executable.startswith('root'):
            self.status = status.SUCCESS


    def outcome(self):
        if self.status == status.SUCCESS:
            return True, ''
        if self.fatalLine:
            return False, 'FatalLine: ' + self.fatalLine
        return False, status.describe(self.status)
