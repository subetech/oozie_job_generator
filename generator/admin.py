from django.contrib import admin

# Register your models here.
from generator.models import Job, TypeReplacements, TableDumpParams, Templates

admin.site.register(Job)
admin.site.register(TypeReplacements)
admin.site.register(TableDumpParams)
admin.site.register(Templates)
