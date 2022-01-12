# -*- coding:utf-8 -*-
#====#====#====#====
# __author__ = "liubc"
#FileName: Reference_Identification.py
#Version:1.0.0
#CreateTime:xxxx-xx-xx
#====#====#====#====
import re
import time
from pymongo import MongoClient
import requests
import itertools


global SIGN
SIGN = False

'''
function:extract links in Bson files from ghtorrent
'''
def link_extraction():
    # connect MongoDB database
    client = MongoClient(unicode_decode_error_handler='ignore')
    db = client.ghtorrent
    # open collection
    coll_input = db['input']
    # new collection
    coll_output = db['output']

    # Building regular expression
    target1 = 'github.com/(.+?)/(.+?)'
    target2 = '(.+?)/(.+?)#(\d+)'
    target3 = '(.+?)/(.+?)@([a-zA-Z0-9]+)'
    # pre-compile
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
            idx = 0
            list_body = str_body.split()
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
                # extract information
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
    global SIGN
    if sign_num == 1:
        list_http = str_full.split("/")
        num_len = len(list_http)
        i = 0
        while i < num_len:
            str_tmp = list_http[i]
            sum_tmp = str_tmp.find('github.com')
            sum_api = str_tmp.find('api.github.com')
            if sum_tmp != -1:
                if (sum_api == -1) and (num_len > i + 2):
                    SIGN = True
                    usr = list_http[i + 1]
                    repo = list_http[i + 2]
                    num_idx = '%d' % idx
                    d['target_org' + num_idx] = usr + '/' + repo
                    d['sample' + num_idx] = str_full
                    break
                elif (sum_tmp != -1) and (num_len > i + 3):
                    SIGN = True
                    usr = list_http[i + 2]
                    repo = list_http[i + 3]
                    num_idx = '%d' % idx
                    d['target_org' + num_idx] = usr + '/' + repo
                    d['sample' + num_idx] = str_full
                    break
            i = i + 1
    if sign_num == 2:
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
Function: Identify changes to project names
Step 1: Extract names for all projects
Step 2: Determine whether the name of each project change. Save the new project name when projecte's name change
Step 3: Replace the old name with the new one
'''
def identify_redirectedProjects():
    idx = 0
    sum = 0
    dict = {}
    dataset = set([])
    f = open('dataset.txt', 'a+')
    # connect MongoDB database
    client = MongoClient(unicode_decode_error_handler='ignore')
    db = client.ghtorrent
    # open collection
    coll_input = db['input']
    for cursor in coll_input.find({}, no_cursor_timeout=True, batch_size=100):
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
        print('traverse tha database：', sum)
    f.close()
    print('The database traversal is complete~')
    print('Total number of projects is：', idx)
    #Step 2
    file = open('repos.txt', 'r', encoding='utf-8')
    data_list = file.readlines()
    file.close()
    # Crawl the project url
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
        # connect MongoDB database
        client = MongoClient(unicode_decode_error_handler='ignore')
        db = client.ghtorrent
        coll_redirected = db['redirected_projects']
        coll_redirected.insert_one(dict)
    replaceNewName()
    return

def replaceNewName():
    # Step 3
    rdictlist = []
    olddict = {}
    newdict = {}
    dict = {}
    # connect MongoDB database
    client = MongoClient(unicode_decode_error_handler='ignore')
    db = client.ghtorrent
    # open collection
    coll_redirected = db['redirected_projects']
    coll_input = db['input']
    coll_output = db['output']
    for curcor in coll_redirected.find({}, no_cursor_timeout=True, batch_size=1000):
        full_name = curcor['full_name']
        new_name = curcor['new_name']
        rdictlist.append(full_name)
        rdictlist.append(new_name)
        olddict[full_name] = new_name
        newdict[new_name] = full_name
    for curcor_in in coll_input.find({}, no_cursor_timeout=True, batch_size=1000):
        source = curcor_in['source_org']
        if source in rdictlist:
            dict = combinedict(source, olddict, newdict)
            coll_3.insert_one(dict)
        else:
            for key in curcor_in:
                if key.find('target_org') != -1:
                    dict = curcor_in[key]
                    target = dict['full_name']
                    if target in rdictlist:
                        dict = combinedict(target, olddict, newdict)
                        coll_output.insert_one(dict)
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
        else:
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

'''Filter project pairs'''
def filter_references():
    # connect MongoDB database
    client = MongoClient(unicode_decode_error_handler='ignore')
    db = client.ghtorrent
    # open collection
    coll_input = db['input']
    coll_output = db['output']
    for cursor in coll_input.find({}, no_cursor_timeout=True, batch_size=1000):
        source_org = cursor['source_org']
        type = cursor['type']
        bSign = False
        d = {}
        for key in cursor:
            re = key.find("target_org")
            if re != -1:
                target_org = cursor[key]
                target_org_new = filtration(target_org)
                if target_org_new != source_org:
                    bSign = True
                    d[key] = target_org_new
        if bSign:
            # orgin:（pullr、pullr_c、issue、issue_c、commit、commit_c）
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
            coll_output.insert_one(d)
    search_repos()
    return

def filtration(target_org):
    target_org_new = ""
    xg_num = target_org.count('/')
    if xg_num != 1:
        return target_org_new
    bool_s = target_org.startswith('/')
    bool_e = target_org.endswith('/')
    if bool_s:
        return target_org_new
    if bool_e:
        return target_org_new
    xg_num = target_org.find('/')
    tmp = target_org[0:xg_num]
    nixu_user = tmp[::-1]
    num = 0
    for letter in nixu_user:
        if (letter.isalnum()) or (letter == '-'):
            num += 1
        else:
            num -= 1
            break
    nixu_user = nixu_user[0:num + 1]
    if nixu_user == "":
        return target_org_new
    user = nixu_user[::-1]
    tmp_repo = target_org[xg_num+1:]
    idx = 0
    for letter in tmp_repo:
        if (letter.isalnum()) or (letter == '-') or (letter == '_') or (letter == '.'):
            idx += 1
        else:
            idx -= 1
            break
    repo = tmp_repo[0:idx + 1]
    if repo != "":
        target_org_new = user + '/' + repo
    return target_org_new

'''
Determine whether the target projects actually exist
'''
def search_repos():
    file = './repos.txt'
    data_set = set([])
    f = open(file, 'r', encoding='utf-8')
    data = [1]
    while data:
        data = f.readline()
        data = data[:-1]
        data_set.add(data)
    f.close()
    # connect MongoDB database
    client = MongoClient(unicode_decode_error_handler='ignore')
    db = client.ghtorrent
    # open collection
    coll = db['input']
    coll_new = db['output']
    data_set = set([])
    sum = 0
    sum_r = 0
    d = {}
    for cursor in coll.find({}, no_cursor_timeout=True, batch_size=100):
        mylist = []
        sign = False
        source_org = cursor['source_org']
        sign = source_org in data_set
        if sign:
            tag = {}
            for key in cursor:
                ret = key.find('target_org')
                if ret != -1:
                    tag[key] = cursor[key]
                    mylist.clear()
            for key in tag:
                str = tag[key]
                if str in data_set:
                    mylist.append(key)
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
                print("Successfully found project pairs：", sum_r)
            else:
                sum += 1
                print("Filter project pairs：", sum)
        else:
            sum += 1
            print("Filter project pairs：", sum)
        s = sum + sum_r
        print("The number of data processed：", s)
    return
