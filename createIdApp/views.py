# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.http import HttpResponseRedirect, HttpResponse
from django.views.decorators.csrf import csrf_exempt
import json
import datetime
import time
import sys


import uuid
from createIdApp.connectionModel import initConnect,getAll_Data,getAll_DetailData,getAll_PinLun,Mssql
from sqlalchemy.orm import sessionmaker

reload(sys)
sys.setdefaultencoding( "utf-8" )

@csrf_exempt
def createPros(request):

    if request.method == 'POST':

        ItemName = request.POST.get('ItemName')
        Validity = request.POST.get('Validity')
        ID = request.POST.get('ID')
        ItemID = uuid.uuid1()
        createTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
        creator = request.POST.get('Creator')

        if len(creator) == 0:
            creators = 'test'
        else:
            creators = creator


        conn = Mssql()

        sql_text = "insert into T_Treasure_EvalCustomItem values ('%s','%s','%s','%d','%d','%s','%s','%d','%s','%s')" % (
            ID, ItemID, ItemName, int(Validity), 1, createTime, '', 1, '', creators)

        conn.exec_non_query(sql_text)

        #跨域问题需要
        response= HttpResponse(json.dumps({'info':'OK'},cls=DateEncoder), content_type="application/json")
        response["Access-Control-Allow-Origin"] = "*"
        response["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS"
        response["Access-Control-Max-Age"] = "1000"
        response["Access-Control-Allow-Headers"] = "*"
        return response



    else:
        allData = []
        name = request.GET.get('Creator')
        res = getAll_Data(name)


        if len(res) == 0:
            return {'Data':'','Creator': ''}
        else:

            for data in res:
                datas = list(data)
                content = {}
                content['id'] = datas[0]
                content['id'] = datas[0]
                content['ItemID'] = str(datas[1])
                content['name'] = datas[2]
                content['Validity'] = datas[3]
                # content['ItemStatus'] = data.ItemStatus
                content['CreateTime'] = datas[5]
                content['Trailer_Tips'] = datas[6]
                content['PollCount'] = datas[4]
                # content['ModifyTime'] = data.ModifyTime
                # content['SkuModifyTime'] = data.SkuModifyTime

                content['Creator'] = datas[9]

                allData.append(content)
            res = {'Data': allData, 'Creator': name}

            response = HttpResponse(json.dumps(res, cls=DateEncoder), content_type="application/json")
            response["Access-Control-Allow-Origin"] = "*"
            response["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS"
            response["Access-Control-Max-Age"] = "1000"
            response["Access-Control-Allow-Headers"] = "*"
            return response

#删除操作
def lsDelete_data(request):
    if request.method == 'POST':
        data = request.POST
        ID = request.POST.get('ID')

        conn = Mssql()

        sql_text = "delete from T_Treasure_EvalCustomItem where ID='%s'"%ID
        conn.exec_non_query(sql_text)

        response = HttpResponse(json.dumps({'info':'OK'},cls=DateEncoder),content_type="application/json")
        response["Access-Control-Allow-Origin"] = "*"
        response["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS"
        response["Access-Control-Max-Age"] = "1000"
        response["Access-Control-Allow-Headers"] = "*"
        return response

    else:
        return HttpResponseRedirect('/')

def lsedit_projects(request):
    if request.method == 'GET':
        ItemID = request.GET.get('ItemID')

        babyID = request.GET.get('babyID')
        res = getAll_DetailData(ItemID)
        all_detailData = []
        if len(res) == 0:
            print '***************%d'%len(res)
            all_detailData.append({'babyID':babyID})
            response = HttpResponse(json.dumps(all_detailData, cls=DateEncoder), content_type="application/json")
            response["Access-Control-Allow-Origin"] = "*"
            response["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS"
            response["Access-Control-Max-Age"] = "1000"
            response["Access-Control-Allow-Headers"] = "*"
            return response
        for data in res:
            datas = list(data)
            content = {}
            content['babyID'] = babyID
            content['id'] = datas[0]
            content['ItemID'] = str(datas[1])
            content['TreasureID'] = datas[2]
            content['TreasureName'] = datas[3]
            content['TreasureLink'] = datas[4]
            content['ShopName'] = datas[5]
            content['Shop_Platform'] = datas[6]
            content['Treasure_Status'] = datas[7]
            content['Monthly_volume'] = datas[8]
            content['IsMerge'] = datas[9]
            content['MergeGuid'] = datas[10]
            content['Category_Name'] = datas[11]
            content['GrpName'] = datas[12]
            content['spuId'] = datas[13]
            content['EvaluationScores'] = str(datas[14])
            content['ShopURL'] = datas[15]
            content['TreasureFileURL'] = datas[16]
            content['Url_No'] = datas[17]
            content['CategoryId'] = str(datas[18])
            content['brandId'] = datas[19]
            content['brand'] = datas[20]
            content['rootCatId'] = str(datas[21])
            content['StyleName'] = datas[22]
            content['CollectionNum'] = datas[23]
            content['ItemName'] = datas[24]
            content['InsertDate'] = datas[25]
            content['ModifyDate'] = datas[26]
            content['ShortName'] = datas[27]
            content['shopID'] = datas[28]

            all_detailData.append(content)


        response = HttpResponse(json.dumps(all_detailData, cls=DateEncoder), content_type="application/json")
        response["Access-Control-Allow-Origin"] = "*"
        response["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS"
        response["Access-Control-Max-Age"] = "1000"
        response["Access-Control-Allow-Headers"] = "*"
        return response

    else:
        return HttpResponseRedirect('/')

#全部宝贝评论
def lsAll_PinLun(request):
    if request.method == 'GET':
        treasureID = request.GET.get('TreasureID')
        itemName = request.GET.get('ItemName')
        res = getAll_PinLun(itemName,treasureID)
        all_PinLun = []
        if len(res) == 0:
            return all_PinLun
        for data in res:
            datas = list(data)
            content = {}

            content['ItemName'] = datas[1]
            content['RateDate'] = datas[0]
            content['TreasureID'] = datas[2]
            content['TreasureName'] = datas[3]
            content['TreasureLink'] = datas[4]
            content['ShopName'] = datas[5]
            content['Category_Name'] = datas[6]
            content['Level_Name'] = datas[7]
            content['AuctionSku'] = datas[8]
            content['DisplayUserNick'] = datas[9]
            content['RateContent'] = datas[10]
            content['IsAppend'] = datas[11]
            content['ImgServiceURL'] = datas[12]

            all_PinLun.append(content)

        response = HttpResponse(json.dumps(all_PinLun,cls=DateEncoder),content_type="application/json")
        response["Access-Control-Allow-Origin"] = "*"
        response["Access-Control-Allow-Methods"] = "POST, GET, OPTIONS"
        response["Access-Control-Max-Age"] = "1000"
        response["Access-Control-Allow-Headers"] = "*"
        return  response

    else:
        return HttpResponseRedirect('/')



class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")
        else:
            return json.JSONEncoder.default(self, obj)



























