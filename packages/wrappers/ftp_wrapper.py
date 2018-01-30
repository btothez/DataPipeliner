import ftplib
import sys

class FtpWrapper:
    def __init__(self, ftp_creds):
        self.dir_list = []
        self.ftp = ftplib.FTP(ftp_creds['host'],
                                ftp_creds['user'],
                                ftp_creds['password'])

    def getFileList(self):
        self.ftp.retrlines('MLSD', self.dir_list.append)
        self.dirList = list(
            filter(lambda filename: 'Type=file' in filename, self.dir_list )
            )

        self.dir_list = list(
            map(lambda filename: filename.split(';')[-1].rstrip().lstrip(), self.dir_list)
            )

    def filterFileListByPattern(self, pattern):
        self.dir_list = list(
            filter(lambda filename: pattern in filename, self.dir_list )
            )

    def disconnectFTP(self):
        self.ftp.quit()
        self.ftp.close()

    def downloadFile(self, filename):
        self.fileLines = []
        with open(filename, "wb") as handle:
            self.ftp.retrbinary('RETR ' + filename, handle.write)

    def __del__(self):
        self.ftp.close()
