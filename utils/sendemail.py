'''
Author: dkl
Date: 2023-12-18 22:27:07
Description: 邮件发送
'''
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from utils.conf import Config


class SendEmail(object):
    """
    发送邮件
    """

    def __init__(self):
        conf = Config("email")
        sender = conf.get_config("sender")
        passwd = conf.get_config("password")
        receiver = conf.get_config("receiver")
        self.sender = sender
        self.passwd = passwd
        self.receiver = receiver
        self.smtpserver = "smtp." + self.sender.split("@")[-1]

    def send_email(self, subject="邮件主题", body="<p>这是个发送邮件的正文</p>", attach=None):
        # 编辑邮件内容
        msg = MIMEMultipart()
        msg["from"] = self.sender
        msg["to"] = self.receiver
        msg["subject"] = subject
        msg.attach(MIMEText(body, "html", "utf-8"))
        if attach is not None:
            file_attach = attach["file"]
            file_name = attach["file_name"]
            # 构造附件
            att = MIMEText(open(file_attach, "rb").read(), "base64", "utf-8")
            att["Content-Type"] = "application/octet-stream"
            att["Content-Disposition"] = 'attachment; filename="%s"' % file_name
            msg.attach(att)
        # 发送邮件
        smtp = smtplib.SMTP_SSL(self.smtpserver, 465)
        # 登录
        smtp.login(self.sender, self.passwd)
        #  发送
        smtp.sendmail(self.sender, self.receiver, msg.as_string())
        # 关闭
        smtp.quit()
