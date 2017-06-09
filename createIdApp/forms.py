#_*_coding:utf-8_*_

from django import forms
from django.core import validators
from django.contrib.auth.models import User
from models import createPro,editPro


#创建项目
class createProject(forms.ModelForm):

    class Meta:
        model = createPro
        fields = ['name','days','IDs','date','ceShidate']

    def save(self, commit=True):

        newTable = createPro.objects.create(
            name = self.data['name'],
            days = self.data['days'],
            IDs = self.data['IDs'],
            date = self.data['date'],
            ceShidate = self.data['ceShidate']
        )

#查看项目
# class editProject(forms.ModelForm):
#     class Meta:
#         model = editPro
#         fields = ['category_name','spec_code','item_number','creater','create_time']



