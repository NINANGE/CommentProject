# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models
import datetime
from django.utils import timezone

# Create your models here.

# class Establish(models.Model):





class createPro(models.Model):
    name = models.CharField(u'项目名称',max_length=200)
    days = models.PositiveIntegerField(u'有效天数')
    IDs = models.CharField(u'宝贝ID',max_length=200)
    date = models.DateTimeField('保存日期')
    ceShidate = models.DateTimeField('测试日期', null=True)


class editPro(models.Model):
    category_name = models.CharField(u'类别',max_length=200)
    spec_code = models.CharField(u'规格编码',max_length=200)
    item_number = models.CharField(u'规格编码',max_length=200)
    creater = models.CharField(u'创建人',max_length=200)
    create_time = models.CharField(u'创建时间',max_length=200)
    son_table = models.ForeignKey(createPro)







