import json

from django.http import HttpResponse
from django.shortcuts import render

# Create your views here.
from django.views.generic import ListView

from generator import models
from django.db import models as dmodels
import generator.main.postgresql_database as postgresql_database
from generator.models import TableDumpParams, Job


def download(request, job_id):
    job = models.Job.objects.filter(id__exact=job_id)[0]
    job_type = job.db_type
    if job_type == 1:
        db = postgresql_database.PostgresqlDatabase()
    else:
        return HttpResponse(json.dumps({"ok": "false", "message": "Database not yet supported!"}))
    db.connect_to_database(job.connection_string)
    db.get_all_tables_from_source(job.schema)
    load_id = db.save_tables_to_database(job)
    return HttpResponse(json.dumps({"ok": "true", "message": "successfully downloaded!", "load_id": load_id}))


class JobView(ListView):
    job_id = None
    load_id = None
    load_ids = None

    def get(self, request, *args, **kwargs):
        self.job_id = kwargs["id"]
        self.load_id = request.GET.get("load_id")
        self.load_ids = TableDumpParams.objects.filter(job__pk=self.job_id).values("load_id").distinct()
        if self.load_id is None:
            self.load_id = self.load_ids.first()["load_id"]
        return super().get(request, *args, **kwargs)

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["load_ids"] = self.load_ids
        return context

    def get_queryset(self):
        return TableDumpParams.objects.filter(job__pk=self.job_id, load_id=self.load_id)