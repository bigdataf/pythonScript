#!/usr/bin/env python
#coding:utf-8

import sys
import commands
import datetime
import re
import sendMail


#设置utf-8模式
reload(sys)
sys.setdefaultencoding( "utf-8")

day = (datetime.datetime.now() + datetime.timedelta(hours=-1)).strftime("%d")
currentday = (datetime.datetime.now() + datetime.timedelta(hours=-1)).strftime("%Y/%m/%d")
currenthour = (datetime.datetime.now() + datetime.timedelta(hours=-1)).strftime("%Y/%m/%d/%H")
currentmonth = (datetime.datetime.now() + datetime.timedelta(hours=-1)).strftime("%Y/%m")

def write_His_Data(file,dd):
    if isinstance(dd, (str, dict)):
        f=open(file,"a+")
        for k,v in dd.items():
            f.write("{}".format(currenthour)+"\t"+str(k)+"\t"+str(v)+"\n")
        f.close()


def getHdfsSize(re_hdfs):
    stat,result=commands.getstatusoutput(re_hdfs)
    h_dict={}
    if stat==0:
        for i in range(0,len(result.split())):
            if i % 3==0 and "hourly" in re_hdfs:
                re_topic = re.search("topics/(.*?)/hourly",result.split()[i+2],re.M|re.I)
                size=result.split()[i]
                topic=re_topic.group(1)
                h_dict[topic]=size
            if i % 3==0 and "daily" in re_hdfs:
                re_topic = re.search("topics/(.*?)/daily",result.split()[i+2],re.M|re.I)
                size=result.split()[i]
                topic=re_topic.group(1)
                h_dict[topic]=size 
        return h_dict
    else:
        return {}

def convertSize(num, tags):
    totals = 0
    if tags == "G":
        totals = float(str(num)) * 1024 * 1024
    elif tags == "M":
        totals = float(num) * 1024
    elif tags == "K":
        totals = float(num)
    return totals


def mergedata():
    all_dict={}
    # dump 基础数据
    reh_base_hdfs = '''hdfs dfs -du  /camus/*/topics/*/hourly/{} |grep -w "{}" '''.format(currentday,currenthour)
    base=getHdfsSize(reh_base_hdfs)
    all_dict.update(base)
    # dump daily 周期的数据
    red_daily_hdfs = '''hdfs dfs -du  /camus/*/topics/*/daily/{} |grep -w "{}" '''.format(currentmonth,day)
    daily=getHdfsSize(red_daily_hdfs)
    all_dict.update(daily)
    print(all_dict)
    return all_dict



def run():
    blacklist_topic="profile"
    sendlist=[u"myname@test.com"]
    all_dict=mergedata()
    for k,v in all_dict.items():
        if int(v) <= 0 and k not in blacklist_topic.split(","):
            sendMgs="Error dump "+str(currenthour)+" 时 topic: "+str(k)+" size: "+str(v)
            sendMail.send_mail(sendlist, "dumpMonitor", str(sendMgs))
    logdir="/data/monitor/dump/log/dump_size.log"
    write_His_Data(logdir,all_dict)


if __name__ == '__main__':
    run()
