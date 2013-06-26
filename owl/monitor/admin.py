from django.contrib import admin
from models import Service, Cluster, Job, Task
from models import HBaseCluster, RegionServer, Table, Region
from models import Counter
from models import Quota

admin.site.register(Service)
admin.site.register(Cluster)
admin.site.register(Job)
admin.site.register(Task)

admin.site.register(HBaseCluster)
admin.site.register(RegionServer)
admin.site.register(Table)
admin.site.register(Region)

admin.site.register(Counter)
admin.site.register(Quota)
