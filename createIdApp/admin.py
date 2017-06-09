# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.contrib import admin
from createIdApp.models import createPro,editPro

# Register your models here.



class createProjectAdmin(admin.ModelAdmin):
    list_display = ('name','days','IDs','date','ceShidate')
admin.site.register(createPro,createProjectAdmin)

class editProjectAdmin(admin.ModelAdmin):
    list_display = ('category_name','spec_code','item_number','creater','create_time')
admin.site.register(editPro,editProjectAdmin)
