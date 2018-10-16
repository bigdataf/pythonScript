#!/bin/env python
#coding:utf-8

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import datetime,time
from email.header import Header
from email.utils import formataddr
import sys


day = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y-%m-%d')
mail_host = "邮件服务器的地址"  # 设置服务器
mail_user = "发送邮件的账号名称"  # 用户名
mail_pass = "发送邮件的账号名称对应的密码"  # 口令
mail_postfix = "域名后缀(如：test.com)"  # 发件箱的后缀

def send_mail(to_list, sub, tabs,filepath=None):
    # me = mail_sender + "<" + mail_user + "@" + mail_postfix + ">"
    me = "%s<%s>" % (Header("日报", 'utf-8'), '邮件发送着账号名')
    msg = MIMEMultipart()
    msg['Subject'] = sub
    msg['From'] = formataddr(["Data Platform",mail_user])
    msg['date'] = time.strftime('%a, %d %b %Y %H:%M:%S %z')
    msg['To'] = ";".join(to_list)
    # 构造html
    d_time = datetime.datetime.now()
    dt = d_time.strftime('%Y-%m-%d %H:%M:%S')
    timezone = dt
    # 构造html
    html = tabs
    # context = MIMEText(content, _subtype='plain', _charset='utf-8')
    context = MIMEText(html, _subtype='html', _charset='utf-8')  # 解决乱码
    msg.attach(context)
    #添加附件
    if filepath is not None:
        att = MIMEText(open(filepath, 'rb').read(), 'xls', 'utf-8')
        att["Content-Type"] = 'application/octet-stream'
        # 这里的filename可以任意写，写什么名字，邮件中显示什么名字
        att["Content-Disposition"] = 'attachment; filename=%s_xls"'%(day)
        msg.attach(att)
    try:
        server = smtplib.SMTP()
        server.connect(mail_host)
        server.ehlo()
        server.esmtp_features['auth'] = 'LOGIN DIGEST-MD5 PLAIN'
        server.login(mail_user, mail_pass)
        server.sendmail(me, to_list, msg.as_string())
        server.close()
        return True
    except Exception, e:
        print str(e)
        return False


def send_message(sendlist):
    global msg
    if sys.argv.__len__() > 1:
        msg = sys.argv[1]
    send_mail(sendlist,"邮件标题",str(msg))


if __name__ == '__main__':
    sendlist=[u"mytest@163.com"]
    send_message(sendlist)
