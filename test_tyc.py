import random
import time

import re

from bs4 import BeautifulSoup

import xlrd

import pymysql

import urllib.parse
import urllib.request


def connDB():
    conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='Kuaisujinru0', db='world', charset='utf8')
    cur = conn.cursor();
    return (conn, cur);


# 更新语句，可执行update,insert语句
def exeUpdate(cur, sql):
    sta = cur.execute(sql)
    conn.commit()
    return (sta);


def exeQuery(cur, sql):  # 查询语句
    cur.execute(sql);
    return (cur);


def connClose(conn, cur):  # 关闭所有连接
    cur.close();
    conn.close();


def DBC2SBC(ustring):
    rstring = ""
    for uchar in ustring:
        inside_code = ord(uchar)
        if inside_code == 0x3000:
            inside_code = 0x0020
        else:
            inside_code -= 0xfee0
        if not (0x0021 <= inside_code and inside_code <= 0x7e):
            rstring += uchar
            continue
        rstring += chr(inside_code)
    return rstring


def SBC2DBC(ustring):
    rstring = ""
    for uchar in ustring:
        inside_code = ord(uchar)
        if inside_code == 0x0020:
            inside_code = 0x3000
        else:
            if not (0x0021 <= inside_code and inside_code <= 0x7e):
                rstring += uchar
                continue
        inside_code += 0xfee0
        rstring += chr(inside_code)
    return rstring

def write_into(logfile, company_name):
    with open(logfile, 'a') as f:
        f.write(company_name + '\n')


def insert_data(sql, datas):
    try:
        cursor = conn.cursor()
        cursor.execute(sql, datas)
        cursor.close()
        conn.commit()
    except Exception as e:
        print('sql出错：' + sql)
        print(datas)
        print(e)
        raise Exception("insert error")


def open_excel(file):
    try:
        book = xlrd.open_workbook(file)
        return book
    except Exception as e:
        print('打开工作簿' + file + '出错：' + str(e))


def read_sheets(file):
    try:
        book = open_excel(file)
        sheets = book.sheets()
        return sheets
    except Exception as e:
        print('读取工作表出错：' + str(e))


def read_data(sheet, n=0):
    dataset = []
    for r in range(sheet.nrows):
        col = sheet.cell(r, n).value
        dataset.append(col)
    return dataset


def get_content(url, waiting=0):
    # waiting = 3 * waiting * random.random() * random.random() + (waiting * random.random() * 2) + 2
    # time.sleep(waiting)
    headers = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        , 'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2'
        , 'Connection': 'keep-alive'
        ,
               'Cookie': 'TYCID=12778aa0359a11e8a7d56ddd8b2588a1; undefined=12778aa0359a11e8a7d56ddd8b2588a1; ssuid=8349659224; Hm_lvt_e92c8d65d92d534b0fc290df538b4758=1525957535; aliyungf_tc=AQAAALrb2BW4DQQAS8zEb4AaDNPdHZsh; csrfToken=wK-2npjUFVjl8acs6MaBIt81; Hm_lpvt_e92c8d65d92d534b0fc290df538b4758=1525958556; RTYCID=d5a12b4e564f4bdbb85a8c3da9415003; bannerFlag=true; tyc-user-info=%257B%2522token%2522%253A%2522eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiIxNTY1MDcwOTMxMCIsImlhdCI6MTUyNTk1ODQ0MywiZXhwIjoxNTQxNTEwNDQzfQ.8PrYS5XNRtUbDtQEy9Bhd_StTyLiPTZTqUGGN_cRy_Ipz_GNigdaxBFoFTqACh3AQlpzF3tFCRahXD5NZ4FN3g%2522%252C%2522integrity%2522%253A%25220%2525%2522%252C%2522state%2522%253A%25220%2522%252C%2522redPoint%2522%253A%25220%2522%252C%2522vipManager%2522%253A%25220%2522%252C%2522vnum%2522%253A%25220%2522%252C%2522onum%2522%253A%25220%2522%252C%2522mobile%2522%253A%252215650709310%2522%257D; auth_token=eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiIxNTY1MDcwOTMxMCIsImlhdCI6MTUyNTk1ODQ0MywiZXhwIjoxNTQxNTEwNDQzfQ.8PrYS5XNRtUbDtQEy9Bhd_StTyLiPTZTqUGGN_cRy_Ipz_GNigdaxBFoFTqACh3AQlpzF3tFCRahXD5NZ4FN3g'
        , 'Host': 'www.tianyancha.com'
        , 'Referer': 'https://www.tianyancha.com/company/639125886'
        , 'Upgrade-Insecure-Requests': '1'
        , 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:59.0) Gecko/20100101 Firefox/59.0'}
    req = urllib.request.Request(url, headers=headers)
    r = urllib.request.urlopen(req)
    content = r.read().decode("utf-8")
    # print(content)
    res_soup = BeautifulSoup(content, "html.parser")
    com_id = url.split('/')[-1]
    if com_id.isdigit():
        return res_soup, com_id
    else:
        return res_soup


def search(keyname):
    key = urllib.parse.quote(keyname)
    search_url = 'http://www.tianyancha.com/search?key=' + key + '&checkFrom=searchBox'

    res_soup = get_content(search_url)

    ifname = res_soup.select('div.search_result_single > div.search_right_item > div > a')
    if len(ifname) > 0:
        r = []
        name = ifname[0].text if len(ifname) > 0 else None
        company_url = ifname[0].get('href')
        r.append(name)
        r.append(company_url)
        return r
    else:
        return None

def get_text(res_soup, tag, i=0):
    text = ''
    list = res_soup.select(tag)
    text = list[i].text.replace("\n", "").replace(" ", "") if len(list) > i else ''
    return text


def tyc_num(text, adict={"0": "0"
    , "3": "1"
    , "4": "2"
    , "9": "3"
    , "6": "4"
    , "2": "5"
    , "1": "6"
    , "8": "7"
    , "7": "8"
    , "5": "9"}):
    rx = re.compile('|'.join(map(re.escape, adict)))

    def one_xlat(match):
        return adict[match.group(0)]

    return rx.sub(one_xlat, text)


def get_basic_info(res_soup, com_id):
    cym = get_text(res_soup, 'div.company_header_width > div > div > div.historyName45Bottom')
    qymc = get_text(res_soup, 'div.company_header_width > div > span')
    fddbr = get_text(res_soup, 'div.human-top > div > div > a')  # 法定代表人
    zczb = tyc_num(get_text(res_soup,
                            'div.baseInfo_model2017 > table > tbody > tr > td > div.new-border-bottom > div.pb10'))  # 注册资本 xx
    zt = get_text(res_soup, 'div.baseInfo_model2017 > table > tbody > tr > td > div > div > div.statusType1')  # 状态
    slrq = tyc_num(
        get_text(res_soup, 'div.baseInfo_model2017 > table > tbody > tr > td > div.new-border-bottom > div.pb10',
                 1))  # 注册日期
    basics = res_soup.select('div.base0910 > table > tbody > tr > td')
    hy = basics[12].text  # 行业
    qyzch = basics[1].text  # 工商注册号
    qylx = basics[8].text  # 企业类型
    zzjgdm = basics[3].text  # 组织机构代码
    yyqx = basics[14].text  # 营业期限
    xkksrq = yyqx.split('至')[0]  # 营业开始日期
    xkjsrq = yyqx.split('至')[1]  # 营业结束日期
    gxdw = basics[18].text  # 登记机构
    hzrq = tyc_num(basics[16].text)  # 核准日期 xx
    tyshxydm = basics[6].text  # 统一社会信用代码
    zs = basics[22].text  # 注册地址
    jyfw = basics[24].text  # 经营范围
    #  内部序号为t+公司id
    datas = (
        't' + str(com_id), qymc, qyzch, fddbr, qylx, zt, zczb, zs, jyfw, gxdw, xkksrq, xkjsrq, slrq, hy, zzjgdm, hzrq,
        tyshxydm, cym)
    sql = '''INSERT INTO tianyancha_qyjc(nbxh, qymc,qyzch,fddbr,qylx,zt,zczb,zs,jyfw,gxdw,xkksrq,xkjsrq,slrq,hy,zzjgdm,hzrq,tyshxydm,cym)
    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
    insert_data(sql, datas)
    return datas


def get_gg_info(res_soup, com_id):
    datass = []
    gglist = res_soup.find(attrs={"id": "_container_staff"}).select('div.clearfix > div > div')
    for i in range(len(gglist)):
        xm = get_text(gglist[i], 'a.overflow-width')
        zw = ''
        for j in range(len(gglist[i].select('div > span'))):
            zw += get_text(gglist[i], 'div > span', j)

        datas = ('t' + str(com_id), xm, zw)
        sql = '''INSERT INTO tianyancha_gg(nbxh,xm,zw) values(%s,%s,%s)'''
        insert_data(sql, datas)
        datass.append(datas)
    return datass


def get_gd_info(res_soup, com_id):
    datass = []
    tzfs = res_soup.find(attrs={"id": "_container_holder"}).select('tbody > tr > td > a')
    for i in range(len(tzfs)):
        tzf_split = tzfs[i].text.replace("\n", "").split()
        tzf = ' '.join(tzf_split)  # 投资方名称
        tzf_id = tzfs[i].get('href').split('/')[-1]  # 投资方url末尾记为tzf_id
        # 如果tzf_id全是数字，则认为是法人，否则为自然人
        if tzf_id.isdigit():
            datas = ('t' + str(tzf_id), tzf, 't' + str(com_id))
        else:
            datas = (str(tzf_id), tzf, 't' + str(com_id))
        sql = '''INSERT INTO tianyancha_tzf(nbxh,xm,btzfnbxh) values(%s,%s,%s)'''
        insert_data(sql, datas)
        datass.append(datas)
    return datass


def get_tz_info(res_soup, com_id):
    datass = []
    btzs = res_soup.select('a.query_name')
    for i in range(len(btzs)):
        btz_name = btzs[i].select('span')[0].text  # 被投资方法人名称
        btz_id = btzs[i].get('href').split('/')[-1]  # 被投资方法人id
        datas = ('t' + str(com_id), btz_name, 't' + str(btz_id))
        sql = '''INSERT INTO tianyancha_tzf(nbxh,xm,btzfnbxh) values(%s,%s,%s)'''
        insert_data(sql, datas)
        datass.append(datas)
    return datass


def main(logfile, company_file, timelog, misslog):
    now = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    write_into(logfile, '\n当前时间：' + now + '\n')
    write_into(timelog, '\n开始执行，当前时间：' + now + '\n')
    sheets = read_sheets(company_file)
    cornames = []
    dataset = read_data(sheets[0], 1)
    cornames.extend(dataset[1:])
    write_into(timelog, 'xls读取完毕' + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())) + '\n')
    qyjc = []
    gg = []
    gd = []
    tz = []

    i = 0
    while i < len(cornames):
        try:
            urlmap = search(cornames[i].encode("utf-8"))
            if urlmap:
                name = urlmap[0]
                url = urlmap[1]
            else:
                url = None
            if url:
                if SBC2DBC(name) == SBC2DBC(cornames[i]):
                    write_into(timelog, cornames[i] + 'url查询完成' + url + '\n')
                    write_into(timelog, '\n当前时间：' + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())) + '\n')
                    soup, company_id = get_content(url)
                    qyjc.append(get_basic_info(soup, company_id))
                    gg.append(get_gg_info(soup, company_id))
                    gd.append(get_gd_info(soup, company_id))
                    tz.append(get_tz_info(soup, company_id))
                    print(cornames[i] + '插入完成' + str(len(qyjc)))
                    write_into(timelog, cornames[i] + '插入完成' + url + '\n')
                    write_into(timelog, '\n当前时间：' + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())) + '\n')
                else:
                    print(cornames[i] + '  查询不一致。' + name)
                    write_into(misslog, cornames[i])
                    write_into(timelog, cornames[i] + 'url查询不一致' + name + '\n')
                    write_into(timelog,'\n当前时间：' + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())) + '\n')

                    soup, company_id = get_content(url)
                    qyjc.append(get_basic_info(soup, company_id))
                    gg.append(get_gg_info(soup, company_id))
                    gd.append(get_gd_info(soup, company_id))
                    tz.append(get_tz_info(soup, company_id))
                    print(cornames[i] + '插入完成' + str(len(qyjc)))
            else:
                print(cornames[i] + '  查询失败。')
                write_into(logfile, cornames[i])
                write_into(timelog, cornames[i] + 'url查询失败\n')
                write_into(timelog, '\n当前时间：' + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())) + '\n')
        except Exception as e:
            print(e)
            print(cornames[i] + str(len(qyjc)))
            write_into(logfile, cornames[i])
            if '302' in str(e):
                a = input('0 continue else exit')
                if a == '0':
                    continue
                else:
                    return -1
        i = i + 1
    print('全部插入完成')
    write_into(timelog, '\n全部插入完成\n当前时间：' + time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())) + '\n')


if __name__ == '__main__':
    timelog = 'time_log.txt'
    logfile = 'failed_log.txt'
    misslog = 'miss_log.txt'
    filename = '../file/com_nam.xlsx'
    conn, cur = connDB();
    main(logfile, filename, timelog, misslog)
    connClose(conn, cur);
