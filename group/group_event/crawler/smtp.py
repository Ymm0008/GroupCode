# coding=utf-8
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import traceback


def send_mail(msg, mode=True):
    # 第三方 SMTP 服务
    mail_host = "smtp.qq.com"  # 设置服务器
    mail_user = "1764691736"  # 用户名
    mail_pass = "pfvxyzqeooexcage"  # 口令

    sender = '1764691736@qq.com'
    receivers = ['zhhhzhang@buaa.edu.cn']
    # receivers = ['1764691736@qq.com']

    message = MIMEText(msg, 'plain', 'utf-8')
    message['From'] = Header("rock", 'utf-8')
    message['To'] = Header("rock", 'utf-8')
    if mode:
        subject = msg
    else:
        subject = msg.splitlines()[-1]
    message['Subject'] = Header(subject, 'utf-8')

    try:
        smtpObj = smtplib.SMTP()
        smtpObj.connect(mail_host, 25)    # 25 为 SMTP 端口号
        smtpObj.login(mail_user, mail_pass)
        smtpObj.sendmail(sender, receivers, message.as_string())
    except:
        traceback.print_exc()
        print "Error: 无法发送邮件"
    else:
        print "邮件发送成功"


def main():
    raise ValueError('hehhehehe')

if __name__ == '__main__':
    try:
        main()
    except:
        error_msg = traceback.format_exc()
        send_mail(msg=error_msg, mode=False)
