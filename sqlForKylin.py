#!/usr/bin env python
# coding:utf-8

import sys
import json
import urllib2
import re, datetime
import base64
import contextlib
import urllib
import logging
import httplib

PY3 = sys.version_info[0] == 3

logger = logging.getLogger(__name__)

reload(sys)
sys.setdefaultencoding('utf-8')

day = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime('%Y-%m-%d')

if PY3:
    def as_unicode(s):
        if isinstance(s, bytes):
            return s.decode('utf-8')
        return str(s)

else:
    def as_unicode(s):
        if isinstance(s, str):
            return s.decode('utf-8')
        return unicode(s)


class Client(object):
    def __init__(self, scheme, host, port, username, password, **kwargs):
        self.scheme = scheme
        self.host = re.sub('/$', '', host)
        self.port = port
        self.username = username
        self.password = password
        self.version = kwargs.get('version', 'v1')
        self.prefix = re.sub('(^/|/$)', '', kwargs.get('prefix', 'kylin/api'))

    def _prepare_url(self, endpoint, query=None):
        if endpoint.startswith('/'):
            endpoint = endpoint[1:]

        url = '{self.scheme}://{self.host}:{self.port}/{self.prefix}/{endpoint}'.format(
            **locals())

        if query:
            url = '{}?{}'.format(url, urllib.parse.urlencode(query))

        return url

    def _prepare_headers(self, method='GET', session=False):
        headers = {
            'User-Agent': 'Kylin Python Client'
        }

        if not session:
            _auth_str = as_unicode('{}:{}').format(
                self.username, self.password)
            _auth = base64.b64encode(
                _auth_str.encode('utf-8')
            ).decode('ascii')
            headers.update({'Authorization': 'Basic {}'.format(_auth)})
        else:
            # todo session
            pass

        if method in ['POST', 'PUT']:
            headers['Content-Type'] = 'application/json'

        return headers

    def _prepare_body(self, body=None):
        if body:
            body = json.dumps(body).encode('utf-8')

        return body

    def fetch(self, endpoint, method='GET', body=None, params=None):
        try:
            method = method.upper()
            url = self._prepare_url(endpoint, params)
            headers = self._prepare_headers(method)
            if self.version == 'v2':
                headers.update(
                    {'Accept': 'application/vnd.apache.kylin-v2+json'})
            body = self._prepare_body(body)

            req = urllib.request.Request(url, headers=headers)
            req.get_method = lambda: method
            logger.debug('''
==========================[QUERY]===============================
 method: %s \n url: %s \n headers: %s \n body: %s
==========================[QUERY]===============================
            ''', method, url, headers, body)

            with contextlib.closing(urllib.request.urlopen(req, body)) as fd:
                try:
                    dumps = json.loads(fd.read().decode("utf-8"))
                except ValueError:
                    logger.debug('KYLIN JSON object could not decoded')

            return dumps

        except urllib.error.HTTPError as e:
            err = e.read()
            try:
                err = json.loads(err.decode("utf-8"))['msg']
            except(ValueError, AttributeError, KeyError):
                logger.debug(err)

            if e.code == 401 and 'User is disabled' in err:
                logger.debug(err)

            if e.code == 401:
                logger.debug(err)

            if e.code == 500:
                logger.debug(err)

        except urllib.error.URLError as e:
            logger.debug(e)


class KylinHttpPost:
    def __init__(self, host, port, username, password):
        self.host = host
        self.port = port
        self.username = username
        self.password = password

    def fetchData(self, params=None):
        if params:
            headers = {
                'User-Agent': 'Kylin Python Client',
                'Content-type': 'application/json;charset=utf-8'
            }
            params = json.dumps(params).encode('utf-8')
            _auth_str = as_unicode('{}:{}').format(
                self.username, self.password)
            _auth = base64.b64encode(
                _auth_str.encode('utf-8')
            ).decode('ascii')
            headers.update({'Authorization': 'Basic {}'.format(_auth)})
            url = 'http://{self.host}:{self.port}/kylin/api/query'.format(**locals())

            '''
            #httplib 处理方式
            address = '{self.host}:{self.port}'.format(**locals())
            conn = httplib.HTTPConnection(address)
            conn.request(method="POST", url=url, body=params, headers=headers)
            response = conn.getresponse()
            res = response.read()
            print(json.loads(res)["results"])

            '''

            req = urllib2.Request(url=url, data=params, headers=headers)
            res_data = urllib2.urlopen(req)
            res = res_data.read()
            return json.loads(res)["results"]


def data_post(data):
    '''
    url post json 数据
    '''
    jdata = json.dumps(data)
    print(jdata)
    header_dict = {"Content-Type": "application/json"}
    url = 'http://hostname/source/import'
    req = urllib2.Request(url=url, data=jdata, headers=header_dict)
    res = urllib2.urlopen(req)
    res = res.read()
    # 返回值
    print(res)


def fetchKylinData(taskID, sql):
    user = "user"
    passwd = "passwd"
    kylinDb = KylinHttpPost(host="hostname", port="7070", username=user, password=passwd)
    body = {
        "sql": sql,
        "offset": 0,
        "limit": 500,
        "acceptPartial": False,
        "project": "olap"
    }
    resultlist = kylinDb.fetchData(body)
    newslist = [str(i[0]) for i in resultlist]
    newsids = []
    count = 0
    for dc in newslist:
        dtmp = {}
        dtmp["id"] = dc
        newsids.append(dtmp)
    # 构造需要的字典结构 转化为json 通过pust方式发送
    data = {
        "taskID": taskID,
        "sourceType": "VIDEO",
        "sourceList": newsids,
        "username": "test"
    }
    return data


def run():
    sql = "select newsid from KYLIN.TEST where day = '%s' order by nub  desc limit 10000" % (
        day)
    data1 = fetchKylinData(92, sql)
    data_post(data1)


if __name__ == '__main__':
    run()
