#!/usr/bin/env python
#coding:utf-8

from kafka import KafkaConsumer
import time
import click
import ConfigParser
import json
import threading
import datetime
import  sched
import schedule
import Queue
import  thread
import sendMail
import  random
# 加载配置
config = ConfigParser.ConfigParser()
# 配置文件
config.read("amon.ini")


pldict = {}
lock = threading.Lock()
def monitor_timer(k,inr,v):
    print('%s %s monitor_timer' % (str(datetime.datetime.now())[0:19],k))   #打印输出
    send_list = config.get("MAIL", "offline").split(",")
    for key, value in pldict.items():
        print(key,value)
        avg = sum(value)/len(value) if len(value) >= 1 else 0
        msg=""
        if  avg > 0:
            pldict[key] = []
            if avg<=v:
                msg = "%s join率 %s 请检查" % (str(k), str(avg))
                sendMail.send_mail(send_list, k, msg)

        else:
            sec=inr/60.0
            msg="%s 近 %s 分钟内没有收到监控数据" %(str(key),str(sec))
        print msg


    global timer  #定义变量
    timer = threading.Timer(inr,monitor_timer,(k,inr,v,))   #60秒调用一次函数
    #定时器构造函数主要有2个参数，第一个参数为时间，第二个参数为函数名
    timer.start()    #启用定时器






@click.group()
def cli():
    pass

@cli.command()
@click.option('--topic',type=str)
@click.option('--offset', type=click.Choice(['smallest', 'earliest', 'largest']))
@click.option("--group",type=str)

def client(topic,offset,group):
    click.echo(topic)
    '''
    kafka consumer
    '''
    consumer = KafkaConsumer(topic,
                             bootstrap_servers=config.get("KAFKA", "Broker_Servers").split(","),
                             group_id=group,
                             auto_offset_reset=offset)
    count = 0
    for message in consumer:
        lock.acquire()
        try:
            count += 1
            if count / 100 == 0:
                click.echo(message.value)
            click.echo(message.value)
            data=json.loads(message.value)

            if not pldict.has_key(data["pipeline"]):
                pldict[data["pipeline"]]=[int(data["value"])]
            else:
                templ=pldict[data["pipeline"]]
                while len(templ)>10:
                    index=random.randint(0, len(templ)-1)
                    del templ[index]
                templ.append(int(data["value"]))
                pldict[data["pipeline"]]=templ
            print(pldict)
        finally:
            lock.release()




'''
类型
执行周期秒
报警阀值

'''

timer = threading.Timer(1,monitor_timer,("cjv",10,10))  #首次启动 10分钟执行一次

if __name__ == '__main__':
    timer.start()
    cli()

