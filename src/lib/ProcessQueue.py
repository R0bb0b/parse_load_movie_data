from subprocess import Popen, PIPE
from tempfile import TemporaryFile #used for standard out

class ProcessQueue:
    dicProcessList = {}#initial process list
    dicActiveProcesses = {}#current active processes
    dicCompletedProcesses = {}#completed processes
    dicProcessOutPuts = {}#standard out from processes
    intLimit = 4#limit number of processes to run at any given time

    def __init__(self, intLimit: int = 4):
        """constructor method

        args:
            intLimit: max number of processes
        """
        self.intLimit = intLimit

    def run(self, dicProcessList: dict):
        """loop until all processes are complete

        args:
            dicProcessList: dictionary of processes to run, example: {
                    "index0":
                        [
                            "program", 
                            "param1", 
                            "param2", 
                            etc...
                        ],
                    "index1":
                        [
                            "program", 
                            "param1", 
                            "param2", 
                            etc...
                        ]
                }
        """
        self.dicProcessList = dicProcessList

        while True:
            self.spawnProcesses()

            self.pollProcesses()

            if(len(self.dicProcessList) == 0 and len(self.dicActiveProcesses) == 0):
                break

    def limitMaxed(self) -> bool:
        """check the current running processes against max concurrent limit

        return: True|False
        """
        return len(self.dicActiveProcesses) >= self.intLimit

    def spawnProcesses(self):
        """spawn processes and migrate current running process from original command dic"""
        if(not self.limitMaxed()):
            for strKey in list(self.dicProcessList):
                lstCmd = self.dicProcessList[strKey]

                self.dicActiveProcesses[strKey] = self.runProcess(lstCmd, strKey)
                self.dicProcessList.pop(strKey)

                if(self.limitMaxed()):
                    break

    def pollProcesses(self):
        """poll processes and migrate completed processes out of current running processes"""
        for strKey in list(self.dicActiveProcesses):
            proc = self.dicActiveProcesses[strKey]

            if proc.poll() is not None:
                (strStdOut, strStdErr) = proc.communicate()

                self.dicProcessOutPuts[strKey].seek(0)
                strStdOut = self.dicProcessOutPuts[strKey].read()
                self.dicProcessOutPuts[strKey].close()

                self.dicCompletedProcesses[strKey] = {"stdout":strStdOut, "stderr":strStdErr, "retcode":proc.returncode}
                self.dicActiveProcesses.pop(strKey)

    def getProcessData(self, strKey: str = False):
        """retrieve proc status, output and errors for each process

        args:
            strKey: index associated with the original running dictionary
            
        return:
            dictionary|False
        """
        if(not strKey):
            return self.dicCompletedProcesses
        elif strKey in self.dicCompletedProcesses:
            return self.dicCompletedProcesses[strKey]
        else:
            return False

    def runProcess(self, lstCmd: list, strKey: str):
        """run a process

        args:
            lstCmd: command to run in list format
            strKey: the index of the command to maintain association

        return: process object pointer
        """
        self.dicProcessOutPuts[strKey] = TemporaryFile()
        return Popen(lstCmd, stdout=self.dicProcessOutPuts[strKey], stderr=PIPE)
