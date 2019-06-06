
import math
import statistics
from datetime import datetime
from pyVmomi import vim
from vspherecapacity.capacity import CapacitySuper, DatabaseAccess
from vspherecapacity.capacity.vsphere import VsphereCapacity
from vspherecapacity.capacity.datastore import DatastoreCapacity, DatastoreClusterCapacity


def _safe_division(x, y):
    if y == 0:
        return 0.0
    return float(x / y)


class ClusterCapacity(CapacitySuper):

    class HACapacity(CapacitySuper):
        def __init__(self, ha_factor, capacity):
            self._mo_id = "{}-ha{}".format(capacity._mo_id, ha_factor)
            self.cluster_ha = '{}-ha_n{}'.format(capacity.name, ha_factor)
            self.cluster = capacity.name
            self.ha_factor = ha_factor
            self.ha_usable_cpu_total = 0.0
            self.ha_usable_mem_total = 0.0
            self.ha_reserved_cpu = 0.0
            self.ha_reserved_mem = 0.0
            self.ha_cpu_over_commit = 0.0
            self.ha_cpu_percent_used = 0.0
            self.ha_mem_over_commit = 0.0
            self.ha_mem_percent_used = 0.0

            self._calculate_ha(capacity)

        def _calculate_ha(self, capacity):
            self.ha_reserved_cpu = _safe_division(capacity.raw_cpu_total, capacity.vmhost_count) * self.ha_factor
            self.ha_reserved_mem = _safe_division(capacity.raw_mem_total, capacity.vmhost_count) * self.ha_factor
            self.ha_usable_cpu_total = capacity.raw_cpu_total - self.ha_reserved_cpu
            self.ha_usable_mem_total = capacity.raw_mem_total - self.ha_reserved_mem
            self.ha_cpu_over_commit = _safe_division(capacity.raw_cpu_used, self.ha_usable_cpu_total)
            self.ha_mem_over_commit = _safe_division(capacity.raw_mem_used, self.ha_usable_mem_total)
            self.ha_cpu_percent_used = self.ha_cpu_over_commit * 100
            self.ha_mem_percent_used = self.ha_mem_over_commit * 100

    def __init__(self, vcenter_name, mo, hw_map=None, sql_map=None):
        # super().__init__()
        self.vcenter_name = vcenter_name
        self.name = mo.name
        self._mo_id = mo._moId
        self.raw_cpu_total = 0.0
        self.raw_mem_total = 0.0
        self.raw_cpu_used = 0.0
        self.raw_mem_used = 0.0
        self.raw_cpu_over_commit = 0  # raw_cpu_used / raw_cpu_total
        self.raw_avg_cpu_over_commit = 0  # all vsp_capacity.cpu_over_commit added together / vmhost_count
        self.raw_mem_over_commit = 0  # raw_mem_used / raw_mem_total
        self.raw_avg_mem_over_commit = 0  # all vsp_capacity.mem_over_commit added together / vmhost_count
        self.raw_storage_total = 0.0
        self.raw_storage_used = 0.0
        self.raw_storage_free = 0.0
        self.vsp_capacity = []
        self.datastores = []
        self.datastore_clusters = []
        self.vmhost_count = len(mo.host)
        self.ha_capacity = []
        self.vm_sizes = []
        self.vm_low_cpu = 0.0
        self.vm_low_mem = 0.0
        self.vm_avg_cpu = 0.0
        self.vm_avg_mem = 0.0
        self.vm_max_cpu = 0.0
        self.vm_max_mem = 0.0

        self.__dbo = None

        self._get_vsp_usage_info(mo, hw_map=hw_map, sql_map=sql_map)
        self._get_datastore_usage_info(mo)
        self._get_usage_info()

    def _get_vsp_usage_info(self, mo, hw_map=None, sql_map=None):
        self.vsp_capacity = [VsphereCapacity(self.vcenter_name, vmhost, hw_map, sql_map) for vmhost in mo.host]
        for c in self.vsp_capacity:
            self.raw_avg_cpu_over_commit += c.cpu_over_commit
            self.raw_avg_mem_over_commit += c.mem_over_commit
        self.raw_avg_cpu_over_commit = _safe_division(self.raw_avg_cpu_over_commit, self.vmhost_count)
        self.raw_avg_mem_over_commit = _safe_division(self.raw_avg_mem_over_commit, self.vmhost_count)

    def _get_datastore_usage_info(self, mo):
        ds_cluster_tracker = {}
        for ds in mo.datastore:
            if not ds.name.find('local') >= 0 and not ds.name.find('datastore') >= 0 and not ds.name.find('swap') >= 0:
                if isinstance(ds.parent, vim.StoragePod):
                    # datastore belongs to a datastore cluster

                    # Check if the ds cluster has already been processed, if so then
                    # all capacity numbers have been gathered, otherwise gather the
                    # capacity information of the ds cluster
                    if not ds_cluster_tracker.get(ds.parent._moId or None):
                        capacity = DatastoreClusterCapacity(ds.parent)
                        self.datastore_clusters.append(capacity)
                        self.raw_storage_total += capacity.total_capacity
                        self.raw_storage_used += capacity.total_used
                        self.raw_storage_free += capacity.total_free_actual
                        ds_cluster_tracker.update({ds.parent._moId: True})
                elif isinstance(ds, vim.Datastore):
                    capacity = DatastoreCapacity(ds)
                    self.datastores.append(capacity)
                    self.raw_storage_total += capacity.total_capacity
                    self.raw_storage_used += capacity.total_used
                    self.raw_storage_free += capacity.total_free_actual

    def _get_usage_info(self):
        vm_cpus = []
        vm_mems = []
        for cap in self.vsp_capacity:
            self.raw_cpu_total += cap.cpu_total
            self.raw_mem_total += cap.mem_total
            self.raw_cpu_used += cap.cpu_used
            self.raw_mem_used += cap.mem_used

            for vm_size in cap.vm_sizes:
                if vm_size not in self.vm_sizes:
                    self.vm_sizes.append(vm_size)
                vm_cpu, vm_mem = vm_size.__str__().split('x')
                vm_cpus.append(int(vm_cpu))
                vm_mems.append(float(vm_mem))
        if not vm_cpus:
            vm_cpus = [0]  # There could be clusters without any VMs, if this is the case then set 0
        if not vm_mems:
            vm_mems = [0]  # There could be clusters without any VMs, if this is the case then set 0
        self.vm_low_cpu = min(vm_cpus)
        self.vm_low_mem = min(vm_mems)
        self.vm_avg_cpu = math.ceil(statistics.mean(vm_cpus))
        self.vm_avg_mem = math.ceil(statistics.mean(vm_mems))
        self.vm_max_cpu = max(vm_cpus)
        self.vm_max_mem = max(vm_mems)
        self.raw_cpu_over_commit = _safe_division(self.raw_cpu_used, self.raw_cpu_total)
        self.raw_mem_over_commit = _safe_division(self.raw_mem_used, self.raw_mem_total)
        self.ha_capacity.append(self.HACapacity(ha_factor=2, capacity=self))
        self.ha_capacity.append(self.HACapacity(ha_factor=1, capacity=self))

    def setup_database_connection(self, db_host, db_name, db_user):
        try:
            self.__dbo = DatabaseAccess(host=db_host,
                                        db=db_name,
                                        user=db_user)
            self.__dbo.initialize_cursor()
        except:
            raise

    def db_update_or_create(self):
        try:
            if not self.__dbo:
                raise ConnectionError(
                    "No connection to a relational database available. "
                    "Must establish database connection first using setup_database_connection()")
            dict_obj = self.convert_to_json()
            dict_obj = dict_obj.copy()

            # process vCenter database as all objects rely upon this
            self.__dbo.update_or_create_dbo(model='capacity_vcentermodel',
                                            obj={'name': dict_obj['vcenter_name']},
                                            where_param='name'
                                            )
            cl_dbo = self.update_cl(dict_obj.copy())

            for vm_size in dict_obj['vm_sizes']:
                self.update_vm_size(vm_size.copy())

            self.update_cl_vmsize(dict_obj.copy(), cl_dbo)

            for ds in dict_obj['datastores']:
                ds_dbo = self.update_datastore(ds.copy())
                self.update_cl_ds(cl_dbo, ds_dbo)

            for ds_cl in dict_obj['datastore_clusters']:
                datastores = ds_cl.pop('datastores')
                dscl_dbo = self.update_datastore_cluster(ds_cl.copy())
                self.update_cl_dscl(cl_dbo, dscl_dbo)
                for ds in datastores:
                    ds_dbo = self.update_datastore(ds)
                    self.update_ds_dscl(ds_dbo, dscl_dbo)
                    self.update_cl_ds(cl_dbo, ds_dbo)

            for ha in dict_obj['ha_capacity']:
                ha_dbo = self.update_ha(ha.copy())
                self.update_cl_ha(cl_dbo, ha_dbo)

            for vsp in dict_obj['vsp_capacity']:
                vsp_dbo = self.update_vsp(vsp.copy())
                self.update_vsp_vmsize(vsp.copy(), vsp_dbo)
                self.update_cl_vsp(cl_dbo, vsp_dbo)

        except ConnectionError as ce:
            raise
        except BaseException as be:
            self.__dbo.dispose()
            raise
        finally:
            self.__dbo.dispose()

    def update_cl_ds(self, cl_dbo, ds_dbo):
        self.__dbo.update_or_create_dbo(model='capacity_clustermodel_datastores',
                                        obj={
                                            'datastoremodel_id': ds_dbo[0][00],
                                            'clustermodel_id': cl_dbo[0][00]
                                        },
                                        where_param='clustermodel_id',
                                        skip_date=True)

    def update_cl_dscl(self, cl_dbo, dscl_dbo):
        self.__dbo.update_or_create_dbo(model='capacity_clustermodel_datastore_clusters',
                                        obj={
                                            'datastoreclustermodel_id': dscl_dbo[0][00],
                                            'clustermodel_id': cl_dbo[0][00]
                                        },
                                        where_param='clustermodel_id',
                                        skip_date=True)

    def update_cl_vsp(self, cl_dbo, vsp_dbo):
        self.__dbo.update_or_create_dbo(model='capacity_clustermodel_vsp_capacity',
                                        obj={
                                            'hostmodel_id': vsp_dbo[0][00],
                                            'clustermodel_id': cl_dbo[0][00]
                                        },
                                        where_param='clustermodel_id',
                                        skip_date=True)

    def update_cl_ha(self, cl_dbo, ha_dbo):
        self.__dbo.update_or_create_dbo(model='capacity_clustermodel_ha_capacity',
                                        obj={
                                            'hamodel_id': ha_dbo[0][00],
                                            'clustermodel_id': cl_dbo[0][00]
                                        },
                                        where_param='clustermodel_id',
                                        skip_date=True)

    def update_cl_vmsize(self, cl_obj, cl_dbo):
        dbo_tracker = []
        for vmsize in cl_obj['vm_sizes']:
            vmsize_dbo = self.__dbo.get_dbo(model='capacity_vmsizemodel',
                                            obj=vmsize,
                                            where_param='vm_size')
            if not vmsize_dbo:
                raise ValueError('vmsize-{} dbo not found'.format(vmsize['vm_size']))

            sql_qry = "SELECT * FROM {} WHERE {}=%s AND {}=%s ;".format('capacity_clustermodel_vm_sizes',
                                                                        'clustermodel_id',
                                                                        'vmsizemodel_id')
            self.__dbo.cursor.execute(sql_qry, (cl_dbo[0][00], vmsize_dbo[0][00]))
            dbo = self.__dbo.cursor.fetchall()

            if not dbo:
                sql_qry = "INSERT INTO {} (clustermodel_id, vmsizemodel_id) VALUES(%s, %s) ;".format(
                    'capacity_clustermodel_vm_sizes')
                self.__dbo.cursor.execute(sql_qry, (cl_dbo[0][00], vmsize_dbo[0][00]))
                self.__dbo.connection.commit()
            sql_qry = "SELECT * FROM {} WHERE {}=%s AND {}=%s ;".format('capacity_clustermodel_vm_sizes',
                                                                        'clustermodel_id',
                                                                        'vmsizemodel_id')
            self.__dbo.cursor.execute(sql_qry, (cl_dbo[0][00], vmsize_dbo[0][00]))
            dbo = self.__dbo.cursor.fetchall()
            for d in dbo:
                dbo_tracker.append(d[0])
        sql_qry = "SELECT * FROM {} WHERE {}=%s ;".format('capacity_clustermodel_vm_sizes',
                                                          'clustermodel_id')
        self.__dbo.cursor.execute(sql_qry, (cl_dbo[0][00],))
        [self.__dbo.remove_dbo(model='capacity_clustermodel_vm_sizes',
                               obj={'id': d[0]},
                               where_param='id') for d in self.__dbo.cursor.fetchall() if d[0] not in dbo_tracker]

    def update_vsp_vmsize(self, vsp, vsp_dbo):
        dbo_tracker = []
        for vmsize in vsp['vm_sizes']:
            vmsize_dbo = self.__dbo.get_dbo(model='capacity_vmsizemodel',
                                            obj=vmsize,
                                            where_param='vm_size')
            if not vmsize_dbo:
                raise ValueError('vmsize-{} dbo not found'.format(vmsize['vm_size']))

            sql_qry = "SELECT * FROM {} WHERE {}=%s AND {}=%s ;".format('capacity_hostmodel_vm_sizes',
                                                                        'hostmodel_id',
                                                                        'vmsizemodel_id')
            self.__dbo.cursor.execute(sql_qry, (vsp_dbo[0][00], vmsize_dbo[0][00]))
            dbo = self.__dbo.cursor.fetchall()

            if not dbo:
                sql_qry = "INSERT INTO {} (hostmodel_id, vmsizemodel_id) VALUES(%s, %s) ;".format('capacity_hostmodel_vm_sizes')
                self.__dbo.cursor.execute(sql_qry, (vsp_dbo[0][00], vmsize_dbo[0][00]))
                self.__dbo.connection.commit()
            sql_qry = "SELECT * FROM {} WHERE {}=%s AND {}=%s ;".format('capacity_hostmodel_vm_sizes',
                                                                        'hostmodel_id',
                                                                        'vmsizemodel_id')
            self.__dbo.cursor.execute(sql_qry, (vsp_dbo[0][00], vmsize_dbo[0][00]))
            dbo = self.__dbo.cursor.fetchall()
            for d in dbo:
                dbo_tracker.append(d[0])
        sql_qry = "SELECT * FROM {} WHERE {}=%s ;".format('capacity_hostmodel_vm_sizes',
                                                          'hostmodel_id')
        self.__dbo.cursor.execute(sql_qry, (vsp_dbo[0][00],))
        [self.__dbo.remove_dbo(model='capacity_hostmodel_vm_sizes',
                               obj={'id': d[0]},
                               where_param='id') for d in self.__dbo.cursor.fetchall() if d[0] not in dbo_tracker]

    def update_cl(self, obj):
        obj.pop('vcenter_name')
        obj.pop('datastores')
        obj.pop('datastore_clusters')
        obj.pop('vsp_capacity')
        obj.pop('ha_capacity')
        obj.pop('vm_sizes')
        obj.pop('_ClusterCapacity__dbo')

        self.__dbo.update_or_create_dbo(model='capacity_clustermodel',
                                        obj=obj,
                                        where_param='_mo_id')
        cl_dbo = self.__dbo.get_dbo(model='capacity_clustermodel',
                                    obj=obj)
        vc_dbo = self.__dbo.get_dbo(model='capacity_vcentermodel',
                                    obj={'name': self.vcenter_name},
                                    where_param='name')
        self.__dbo.update_or_create_dbo(model='capacity_clustermodel_vcenter',
                                        obj={
                                            'vcentermodel_id': vc_dbo[0][00],
                                            'clustermodel_id': cl_dbo[0][00]
                                        },
                                        where_param='clustermodel_id',
                                        skip_date=True)
        return cl_dbo

    def update_vm_size(self, obj):
        self.__dbo.update_or_create_dbo(model='capacity_vmsizemodel',
                                        obj=obj,
                                        where_param='vm_size',
                                        skip_date=True)

    def update_vsp(self, obj):
        obj.pop('vcenter')
        obj.pop('cluster')
        obj.pop('_parent_id')
        obj.pop('virtualmachine')  # Todo Remove this once VirtualMachineCapacity is implemented
        obj.pop('vm_sizes')

        self.__dbo.update_or_create_dbo(model='capacity_hostmodel',
                                        obj=obj,
                                        where_param='_mo_id')
        vsp_dbo = self.__dbo.get_dbo(model='capacity_hostmodel',
                                     obj=obj)
        vc_dbo = self.__dbo.get_dbo(model='capacity_vcentermodel',
                                    obj={'name': self.vcenter_name},
                                    where_param='name')
        self.__dbo.update_or_create_dbo(model='capacity_hostmodel_vcenter',
                                        obj={
                                            'vcentermodel_id': vc_dbo[0][00],
                                            'hostmodel_id': vsp_dbo[0][00]
                                        },
                                        where_param='hostmodel_id',
                                        skip_date=True)
        return vsp_dbo

    def update_ha(self, obj):
        self.__dbo.update_or_create_dbo(model='capacity_hamodel',
                                        obj=obj,
                                        where_param='_mo_id')
        ha_dbo = self.__dbo.get_dbo(model='capacity_hamodel',
                                    obj=obj)
        vc_dbo = self.__dbo.get_dbo(model='capacity_vcentermodel',
                                    obj={'name': self.vcenter_name},
                                    where_param='name')
        self.__dbo.update_or_create_dbo(model='capacity_hamodel_vcenter',
                                        obj={
                                            'vcentermodel_id': vc_dbo[0][00],
                                            'hamodel_id': ha_dbo[0][00]
                                        },
                                        where_param='hamodel_id',
                                        skip_date=True)
        return ha_dbo

    def update_datastore(self, obj):
        obj.pop('_mo_type')
        self.__dbo.update_or_create_dbo(model='capacity_datastoremodel',
                                        obj=obj,
                                        where_param='_mo_id')
        ds_dbo = self.__dbo.get_dbo(model='capacity_datastoremodel',
                                    obj=obj)
        vc_dbo = self.__dbo.get_dbo(model='capacity_vcentermodel',
                                    obj={'name': self.vcenter_name},
                                    where_param='name')
        self.__dbo.update_or_create_dbo(model='capacity_datastoremodel_vcenter',
                                        obj={
                                            'vcentermodel_id': vc_dbo[0][00],
                                            'datastoremodel_id': ds_dbo[0][00]
                                        },
                                        where_param='datastoremodel_id',
                                        skip_date=True)
        return ds_dbo

    def update_datastore_cluster(self, obj):
        obj.pop('_mo_type')
        self.__dbo.update_or_create_dbo(model='capacity_datastoreclustermodel',
                                        obj=obj,
                                        where_param='_mo_id')
        dscl_dbo = self.__dbo.get_dbo(model='capacity_datastoreclustermodel',
                                      obj=obj)
        vc_dbo = self.__dbo.get_dbo(model='capacity_vcentermodel',
                                    obj={'name': self.vcenter_name},
                                    where_param='name')
        self.__dbo.update_or_create_dbo(model='capacity_datastoreclustermodel_vcenter',
                                        obj={
                                            'vcentermodel_id': vc_dbo[0][00],
                                            'datastoreclustermodel_id': dscl_dbo[0][00]
                                        },
                                        where_param='datastoreclustermodel_id',
                                        skip_date=True)
        return dscl_dbo

    def update_ds_dscl(self, ds_dbo, dscl_dbo):
        self.__dbo.update_or_create_dbo(model='capacity_datastoreclustermodel_datastores',
                                        obj={
                                            'datastoremodel_id': ds_dbo[0][00],
                                            'datastoreclustermodel_id': dscl_dbo[0][00]
                                        },
                                        where_param='datastoreclustermodel_id',
                                        skip_date=True)

    @staticmethod
    def _is_lowest_level(obj):
        for key in list(obj.keys()):
            if isinstance(obj[key], list):
                # not to the root yet
                return key
        return "lowest"

    @staticmethod
    def _get_lowest_level(obj):
        if ClusterCapacity._is_lowest_level(obj) == "lowest":
            # found the lowest level
            return
        else:
            return ClusterCapacity

