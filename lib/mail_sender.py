#!/bin/env python3

import  binascii, sys, os
import keyring
from base64 import decodestring
from os.path import basename,  join
from datetime import date
import requests
from exchangelib import DELEGATE, Account, Credentials, Configuration, NTLM, Message, FileAttachment, Mailbox
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from exchangelib.protocol import BaseProtocol, NoVerifyHTTPAdapter
BaseProtocol.HTTP_ADAPTER_CLS = NoVerifyHTTPAdapter

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
workdir = 'C:\\Temp' if 'win' in sys.platform else '/tmp'

def normpath(filename): return filename.replace('/', '\\')
def unixpath(filename): return filename.replace('\\','/')
def is_rar(filename): return os.path.splitext(filename)[1].lower() == '.rar'
def is_xlsx(filename): return os.path.splitext(filename)[1].lower() == '.xlsx'
def is_xlsb(filename): return os.path.splitext(filename)[1].lower() == '.xlsb'
def is_csv(filename): return os.path.splitext(filename)[1].lower() == '.csv'
def is_xls(filename): return os.path.splitext(filename)[1].lower() == '.xls'
def is_zip(filename): return os.path.splitext(filename)[1].lower() == '.zip'
def is_pptx(filename): return os.path.splitext(filename)[1].lower() == '.pptx'


def ds(val):
    if val.endswith('=\n'):
        try:
            return decodestring(val)
        except binascii.Error:
            return val
    else:
        return val

class Authorization(object):
    SERVICE_NAME = 'iskra'
    def __init__(self, user, domain, mailbox, server):
        self.kr = keyring.get_keyring()
        self.user = user
        self.domain = domain
        self.mailbox = mailbox
        self.server = server

    def get_password(self):
        p = self.kr.get_password(self.SERVICE_NAME, self.user)
        if not p:
            raise Exception("No password!")
        
        return p

    @property
    def username(self):
        return '%s\\%s' % (self.domain, self.user)

    @property
    def password(self):
        return self.get_password()


class MailReceiver(object):
    savepath = join(workdir, "New")
    subject = None
    filter_date = date.today().isoformat()
    sleeptime = 30

    def __init__(self, password=None, savepath=None, auth_class=None):

        self.pas = password or self.AuthClass.password
        self.savepath = savepath or workdir
        self.AuthClass = auth_class
        self.creds = Credentials(username=self.AuthClass.username, password=ds(self.pas))


        self.config = Configuration(server=self.AuthClass.server,
                                    credentials = self.creds,
                                    auth_type=NTLM)

        self.account = Account( primary_smtp_address=self.AuthClass.mailbox,
                                config = self.config,
                                credentials = self.creds,
                                autodiscover = False,
                                access_type = DELEGATE )


        self.__files = []
        self.__msgs = []



    def send_message(self, recipients, theme, body, files=[]):

        m = Message(
            account = self.account,
            folder = self.account.sent,
            subject = theme,
            body = body,
            to_recipients = [Mailbox(email_address=r) for r in recipients],
        )

        for f in files:
            fa = FileAttachment(name=basename(f), content=open(f,'rb').read())
            m.attach(fa)

        m.send_and_save() 
    
    def __saveToFile(self, filename, content):
        if is_rar(filename) or is_xls(filename) or is_xlsx(filename) or is_zip(filename) or is_xlsb(filename) or is_csv(filename):
            fn = filename
            print(fn)
            f = open(fn, 'wb')
            f.write(content)
            f.close()
            return True
        else:
            return False

    def get_message_attachments(self, message, savepath):
        """
        Сохранение всех вложений из сообщения
        
        message: object
            Сообщение
        savepath: str
           Путь для сохранения        
        """
        filenames = []
        if message.attachments:
            for attachment in message.attachments:
                if isinstance(attachment, FileAttachment):
                    if 'win' in sys.platform:
                        filename = normpath(join(savepath, basename(attachment.name)))
                    else:
                        filename = unixpath(join(savepath, basename(attachment.name)))

                    if self.__saveToFile(filename, attachment.content):
                        filenames.append(attachment.name)
        return filenames
    
    
    def get_folder_messages(self, folder_name, is_read=None, filter_date=None, subject=None, author=None):
        """
        Получение сообщений из папки в почте
        
        folder_name:str
            Имя папки
        is_read:bool
           Чтение только прочитанных\не прочитанных сообщений
        filter_date:str
            Дата
        subject:str
            Тема
        author:str 
            Отправитель
        """
        params = {}
        if filter_date:
            params['datetime_received__gt'] = filter_date.isoformat()
        if subject:
            params['subject__contains'] = subject
        if author:
            params['author__contains'] = author
        if is_read!=None:
            params['is_read']=is_read

        messages = []
        if True:         
            for message in (self.account.inbox/folder_name).filter(**params).only('subject', 'body','author','datetime_sent', 'is_read', 'attachments').iterator():
                messages.append(message)

        return messages