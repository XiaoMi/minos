# Create your views here.
import hashlib
import os
import time

from django.http import HttpResponse
from django.http import HttpResponseRedirect
from django.shortcuts import render_to_response
from django.views.decorators.csrf import csrf_exempt

from package_server.forms import UploadFileForm
from package_server.models import Package
from tank.settings import STATIC_URL

ITEM_LIMITS = 20

@csrf_exempt
def upload_package(request):
  if request.method == 'POST':
    form = UploadFileForm(request.POST, request.FILES)
    if form.is_valid():
      artifact = request.POST.get('artifact')
      revision_no = request.POST.get('revision')
      file_obj = request.FILES.get('file')

      error_message = str()
      if not artifact:
        error_message = 'Artifact should not be empty'
      elif not revision_no:
        error_message = 'Revison should not be empty'

      if error_message:
        return render_to_response('upload.html', {
          'error_message': error_message,
          'STATIC_URL': STATIC_URL.rstrip('/'),
        })
      else:
        package_name = os.path.basename(file_obj.name)
        checksum = generate_checksum(file_obj)
        time = generate_timestamp()
        file_obj.seek(0)
        package = Package(artifact=artifact, name=package_name,
            revision=revision_no, timestamp=time,
            checksum=checksum, file=file_obj)
        package.save()
        return render_to_response('upload.html', {
          'upload_success': True,
          'package': package,
          'STATIC_URL': STATIC_URL.rstrip('/'),
        })
  else:
    form = UploadFileForm()

  return render_to_response('upload.html', {
      'form': form,
      'STATIC_URL': STATIC_URL.rstrip('/'),
  })

def list_packages(request, page_no = 1):
  package_list = Package.objects.order_by('id').reverse()
  has_package = (len(package_list) > 0)
  return render_to_response('package_list.html', {
      'package_list': package_list,
      'has_package': has_package,
      'STATIC_URL': STATIC_URL.rstrip('/'),
  })

def check_package(request):
  artifact = request.GET.get('artifact')
  checksum = request.GET.get('checksum')

  package = get_package(artifact, checksum)
  if package:
    return HttpResponse(str(package))
  else:
    return HttpResponse('Package Not Found')

def get_latest_package_info(request):
  artifact = request.GET.get('artifact')
  package_name = request.GET.get('package_name')
  package = get_latest_package(artifact, package_name)
  if package:
    return HttpResponse(str(package))
  else:
    return HttpResponse('Package Not Found')

def generate_checksum(fp):
  sha1 = hashlib.sha1()
  while True:
    buffer = fp.read(4096)
    if not buffer: break
    sha1.update(buffer)
  return sha1.hexdigest()

def generate_timestamp():
  return time.strftime('%Y%m%d-%H%M%S')

def get_latest_package(artifact, package_name):
  if package_name:
    package_list = Package.objects.filter(
      artifact=artifact, name=package_name,
    ).order_by('id').reverse()
  else:
    package_list = Package.objects.filter(
      artifact=artifact,
    ).order_by('id').reverse()

  if len(package_list) > 0:
    return package_list[0]
  else:
    return None

def get_package(artifact, checksum):
  package_list = Package.objects.filter(
      artifact=artifact,
      checksum=checksum,
  )

  if len(package_list) > 0:
    return package_list[0]
  else:
    return None

