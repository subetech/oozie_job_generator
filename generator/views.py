from django.shortcuts import render

# Create your views here.
from generator import models


def postgres(request, job_id):
    job = models.Job.objects.filter(id__exact=job_id)
