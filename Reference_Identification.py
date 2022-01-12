# -*- coding:utf-8 -*-
#====#====#====#====
# __author__ = "liubc"
#FileName: Reference_Identification.py
#Version:1.0.0
#CreateTime:xxxx-xx-xx
#====#====#====#====

#导入库
import re
import time
#导入pymongo库
from pymongo import MongoClient
import openpyxl
import json
import requests
import itertools
import time

global SIGN
SIGN = False

'''
功能：从ghtorrent平台存储的github数据中提取引用
步骤：
1. 依次遍历每个文档中的"body"信息，使用正则表达式'(.+?)/(.+?)[#@](.+?)'检查文本信息是否满足3种匹配模式的条件
2. 针对满足条件的文档，抽取数据：user、repo等
3. 遍历数据库直到结束
'''
def link_extraction():
    # 连接MongoDB数据库-默认链接
    client = MongoClient(unicode_decode_error_handler='ignore')
    # 连接数据库ghtorrent
    db = client.ghtorrent
    # 打开集合
    coll_input = db['data']
    # 新建集合
    coll_output = db['new_data']

    # 正则表达式
    target1 = 'github.com/(.+?)/(.+?)'
    target2 = '(.+?)/(.+?)#(\d+)'
    target3 = '(.+?)/(.+?)@([a-zA-Z0-9]+)'
    # 预先编译
    pattern1 = re.compile(target1)
    pattern2 = re.compile(target2)
    pattern3 = re.compile(target3)
    patterns = (pattern1, pattern2, pattern3)
    i = 0
    pattern_num = 0
    lenmax = len(patterns)
    while pattern_num < lenmax:
        pattern = patterns[pattern_num]
        for cursor in coll_input.find({'body': {'$regex': pattern}}, no_cursor_timeout=True, batch_size=1000):
            d = {}
            SIGN = False
            str_body = cursor['body']
            sign_num = pattern_num + 1
            # 满足条件，向下继续执行
            idx = 0
            # 'body'字符串切分
            list_body = str_body.split()
            # 针对每一个子字符串
            for str_full in list_body:
                num_re = -1
                if sign_num == 1:
                    num_re = str_full.find('github.com')
                elif sign_num == 2:
                    if (str_full.find('github.com')) == -1:
                        num_re = str_full.find('#')
                    else:
                        continue
                elif sign_num == 3:
                    if (str_full.find('github.com')) == -1:
                        num_re = str_full.find('@')
                    else:
                        continue
                else:
                    continue
                if num_re != -1:
                    try:
                        d = AnalysisData(str_full, sign_num, idx, d)
                        idx = idx + 1
                    except:
                        continue
            if SIGN:
                # 提取当前文档的源repo及其他相关信息
                d['type'] = sign_num
                d['sourceId'] = cursor['_id']
                d['updated_at'] = cursor['updated_at']
                # pull_request_comments
                d['_links'] = cursor['_links']
                sourceRepo = cursor['repo']
                sourceUser = cursor['owner']
                d['source_org'] = sourceUser + '/' + sourceRepo
                coll_output.insert_one(d)
                i = i + 1
        pattern_num = pattern_num + 1
    return

def AnalysisData(str_full, sign_num, idx, d):
    global SIGN  # 函数中声明全局变量
    if sign_num == 1:
        list_http = str_full.split("/")
        num_len = len(list_http)
        i = 0
        while i < num_len:
            str_tmp = list_http[i]
            sum_tmp = str_tmp.find('github.com')
            sum_api = str_tmp.find('api.github.com')
            if sum_tmp != -1:
                if (sum_api == -1) and (num_len > i + 2):#格式为github.com
                    SIGN = True
                    usr = list_http[i + 1]
                    repo = list_http[i + 2]
                    num_idx = '%d' % idx
                    d['target_org' + num_idx] = usr + '/' + repo
                    d['sample' + num_idx] = str_full
                    break
                elif (sum_tmp != -1) and (num_len > i + 3):#格式为api.github.com
                    SIGN = True
                    usr = list_http[i + 2]
                    repo = list_http[i + 3]
                    num_idx = '%d' % idx
                    d['target_org' + num_idx] = usr + '/' + repo
                    d['sample' + num_idx] = str_full
                    break
            i = i + 1
    if sign_num == 2:
        # 对#切片
        list_j = str_full.split("#")
        if list_j:
            str_xg = list_j[0]
            list_xg = str_xg.split("/")
            if len(list_xg) > 1:
                SIGN = True
                usr = list_xg[-2]
                repo = list_xg[-1]
                num_idx = '%d' % idx
                d['target_org' + num_idx] = usr + '/' + repo
                d['sample' + num_idx] = str_full
    if sign_num == 3:
        # 对@切片
        list_j = str_full.split("@")
        if list_j:
            str_xg = list_j[0]
            list_xg = str_xg.split("/")
            if len(list_xg) > 1:
                SIGN = True
                usr = list_xg[-2]
                repo = list_xg[-1]
                num_idx = '%d' % idx
                d['target_org' + num_idx] = usr + '/' + repo
                d['sample' + num_idx] = str_full
    return d

'''
功能：识别项目名称的修改
步骤1：提取所有的项目
步骤2：判断每个项目是否有跳转现象，若有则保存新的项目名称
步骤3：把跳转后的项目名称同一修改为跳转前的项目名称
'''
def identify_redirectedProjects(coll):
    # 提取所有的项目
    idx = 0
    sum = 0
    dict = {}
    dataset = set([])
    f = open('dataset.txt', 'a+')
    print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    for cursor in coll.find({}, no_cursor_timeout=True, batch_size=100):
        ID = cursor['_id']
        source = cursor['source_org']
        if source not in dataset:
            dataset.add(source)
            dict['_id'] = ID
            dict['full_name'] = source
            source += '\n'
            f.write(source)
            idx += 1
        for key in cursor:
            if key.find('target_org') != -1:
                tag = cursor[key]
                target = tag['full_name']
                if target not in dataset:
                    dataset.add(target)
                    target += '\n'
                    f.write(target)
                    idx += 1
        sum += 1
        print('遍历数据表：', sum)
    f.close()
    print('数据库遍历完成')
    print('项目总数为：', idx)
    #判断每个项目是否有跳转现象，若有则保存新的项目名称
    # 读取项目
    file = open('dataset.txt', 'r', encoding='utf-8')
    data_list = file.readlines()
    file.close()
    # 依次爬取项目网址
    for str in data_list:
        repo = str[:-1]
        repo_list = siteCrawl(repo)
        dict['status_code'] = repo_list['status_code']
        ret = repo_list.get('redirection')
        if ret != None:
            dict['redirection'] = repo_list['redirection']
        newname = repo_list.get('new_name')
        if newname != None:
            dict['new_name'] = repo_list['new_name']
        dict['crawler'] = True
        # 连接MongoDB数据库-默认链接
        client = MongoClient(unicode_decode_error_handler='ignore')
        # 连接数据库ghtorrent
        db = client.ghtorrent
        coll_new = db['redirected_projects']
        coll_new.insert_one(dict)
    # 把跳转后的项目名称同一修改为跳转前的项目名称
    rdictlist = []
    olddict = {}
    newdict = {}
    dict = {}
    coll_1 = db['data_01']
    coll_2 = db['data_02']
    coll_3= db['data_03']
    for curcor in coll_1.find({}, no_cursor_timeout=True, batch_size=1000):
        full_name = curcor['full_name']
        new_name = curcor['new_name']
        rdictlist.append(full_name)
        rdictlist.append(new_name)
        olddict[full_name] = new_name
        newdict[new_name] = full_name
    for curcor in coll_2.find({}, no_cursor_timeout=True, batch_size=1000):
        source = curcor['source_org']
        if source in rdictlist:
            dict = combinedict(source, olddict, newdict)
            coll_3.insert_one(dict)
        else:
            for key in curcor:
                if key.find('target_org') != -1:
                    dict = curcor[key]
                    target = dict['full_name']
                    if target in rdictlist:
                        dict = combinedict(target, olddict, newdict)
                        coll_3.insert_one(dict)
    return


def siteCrawl(full_name):
    token_list = [
        '915544cbbb05b34ca25803bc18f829b0912eba9e',
        'f47cdf56353f15afd987748873d9cbde1caafecb'
    ]
    token_iter = itertools.cycle(token_list)
    token = token_iter.__next__()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:36.0) Gecko/20100101 Firefox/36.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8;application/vnd.github.v3.star+json',
        'Accept-Language': 'en',
        'Authorization': 'token ' + token
    }
    dict = {}
    url = 'https://api.github.com/repos/' + full_name
    re = requests.get(url, headers=headers, timeout=3)
    status_code = re.status_code
    if status_code == 200:
        response_dict = re.json()
        if 'html_url' in response_dict:
            html = response_dict['html_url']
            mylist = html.split('/')
            new_name = mylist[3] + '/' + mylist[4]
        else:#404:在当前状态码下，令新旧项目名一致
            new_name = full_name
        redirection = 'False'
        if new_name != full_name:
            redirection = 'True'
        dict['full_name'] = full_name
        dict['status_code'] = status_code
        dict['redirection'] = redirection
        dict['new_name'] = new_name
    return dict

def combinedict(name, olddict, newdict):
    dict = {}
    ret = olddict.get(name)
    if ret == None:
        ret = newdict.get(name)
        dict['full_name'] = ret
        dict['new_name'] = name
    else:
        dict['full_name'] = name
        dict['new_name'] = ret
    return dict

'''依赖项目对提取步骤：
#1.过滤项目对
    #1.1.过滤虚假的引用项目
    #1.2.过滤相同的项目对（内部调用）
#2.在repos中提取（去除fork项目）
    #2.1.判断源项目是否存在
    #2.2.判断目标项目是否存在
'''
def filter_references(coll):
    for cursor in coll.find({}, no_cursor_timeout=True, batch_size=1000):
        source_org = cursor['source_org']
        type = cursor['type']
        bSign = False
        d = {}
        for key in cursor:
            re = key.find("target_org")
            if re != -1:
                target_org = cursor[key]
                # 过滤字符串
                target_org_new = filtration(target_org)
                # 剔除相同的引用项目和源项目对
                if target_org_new != source_org:
                    bSign = True
                    d[key] = target_org_new
        if bSign:
            # orgin:来源（pullr、pullr_c、issue、issue_c、commit、commit_c）
            orign = "pullr"
            d['orgin'] = orign
            d['source_org'] = source_org
            d['sourceId'] = cursor['_id']
            d['type'] = cursor['type']
            d['updated_at'] = cursor['updated_at']
            if (orign == "pullr_c") or (orign == "pullr"):
                d['_links'] = cursor['_links']
            elif (orign == "commit_c") or (orign == "issue"):
                d['html_url'] = cursor['html_url']
            elif (orign == "commit") or (orign == "issue_c"):
                d['url'] = cursor['url']
            coll.insert_one(d)
            i = i + 1
            print("文档总数为： ", i)
            print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    # 查看引用项目是否真实存在
    search_repos()
    return

def filtration(target_org):
    target_org_new = ""
    # 符合条件的字符串中只可能包含一个'/'
    xg_num = target_org.count('/')
    if xg_num != 1:
        return target_org_new
    # 用户名和项目名不能为空，即'/'不能在首位和尾部
    bool_s = target_org.startswith('/')
    bool_e = target_org.endswith('/')
    if bool_s:
        return target_org_new
    if bool_e:
        return target_org_new
    #开始处理
    xg_num = target_org.find('/')
    #第一步：判断用户名是否满足条件(只能包含数字、字母或单个连字符”-”（"_”也不行），且连字符”-”不能在开头和结尾)
    #截取“user”字符串，然后翻转字符串，从左向右查找第一个非数字、字母及"-"的字符，将该部分字符串再次翻转后即为有效用户名
    tmp = target_org[0:xg_num]
    nixu_user = tmp[::-1]
    num = 0
    for letter in nixu_user:
        if (letter.isalnum()) or (letter == '-'):
            num += 1 # +1的目的是取下一个字符
        else:
            num -= 1  # -1的目的是取上一个字符
            break
    nixu_user = nixu_user[0:num + 1]
    #截取后的用户名不能为空
    if nixu_user == "":
        return target_org_new
    user = nixu_user[::-1]
    # 第二步：判断项目名是否满足条件(除数字、字母外只能包含点“.”以及连字符”-”或”_”(不限开头和结尾))
    # 截取“repo”字符串，从左向右查找第一个不是数字、字母、"-"、"_"以及"."的字符，将该部分字符串即为有效项目名
    tmp_repo = target_org[xg_num+1:]
    idx = 0
    for letter in tmp_repo:
        if (letter.isalnum()) or (letter == '-') or (letter == '_') or (letter == '.'):
            idx += 1# +1的目的是取下一个字符
        else:
            idx -= 1# -1的目的是取上一个字符
            break
    repo = tmp_repo[0:idx + 1]
    # 截取后的项目名不能为空
    if repo != "":
        target_org_new = user + '/' + repo
    return target_org_new

'''
查看引用项目是否真实存在
'''
def search_repos():
    file = '‪./repos.txt'
    data_set = set([])
    # 读取文件中的repo
    f = open(file, 'r', encoding='utf-8')
    data = [1]
    while data:
        data = f.readline()
        data = data[:-1]
        data_set.add(data)
    f.close()
    # 连接MongoDB数据库-默认链接
    client = MongoClient(unicode_decode_error_handler='ignore')
    # 连接数据库ghtorrent
    db = client.ghtorrent
    coll = db['data']
    coll_new = db['data_new']
    data_set = set([])
    sum = 0
    sum_r = 0
    d = {}
    for cursor in coll.find({}, no_cursor_timeout=True, batch_size=100):
        mylist = []
        sign = False
        source_org = cursor['source_org']
        # 先判断源项目是否存在
        sign = source_org in data_set
        if sign:
            tag = {}
            for key in cursor:
                ret = key.find('target_org')
                if ret != -1:
                    tag[key] = cursor[key]
                    # 再判断引用项目是否存在
                    mylist.clear()
            for key in tag:
                str = tag[key]
                if str in data_set:
                    # 若引用项目存在，则将对应键存储到列表中
                    mylist.append(key)
            # 遍历完所有Repo后，判断源项目和引用项目是否存在
            if len(mylist) > 0:
                d.clear()
                d['_id'] = cursor['_id']
                d['source_org'] = cursor['source_org']
                for x in mylist:
                    dic = {}
                    dic['full_name'] = cursor[x]
                    origin = cursor['orgin']
                    dic['origin'] = origin
                    dic['type'] = cursor['type']
                    dic['updated_at'] = cursor['updated_at']
                    dic['sourceId'] = cursor['sourceId']
                    if (origin == "pullr_c") or (origin == "pullr"):
                        dic['_links'] = cursor['_links']
                    elif (origin == "commit_c") or (origin == "issue"):
                        dic['html_url'] = cursor['html_url']
                    elif (origin == "commit") or (origin == "issue_c"):
                        dic['url'] = cursor['url']
                    d[x] = dic
                coll_new.insert_one(d)
                sum_r += 1
                print("成功找到项目对：", sum_r)
            else:
                sum += 1
                print("过滤项目对：", sum)
        else:
            sum += 1
            print("过滤项目对：", sum)
        s = sum + sum_r
        print("共处理数据条目为：", s)
    return