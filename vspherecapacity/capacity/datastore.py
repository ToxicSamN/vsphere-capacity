
from pyVmomi import vim
from vspherecapacity.capacity import CapacitySuper
from vspherecapacity.capacity.byteconversion import ByteFactors


class DatastoreCapacity(CapacitySuper):

    def __init__(self, mo):
        self.name = mo.name
        self._mo_id = mo._moId
        self._mo_type = 'Datastore'
        self.total_capacity = float(mo.summary.capacity or 0) / ByteFactors.KB_to_TB
        self.total_free_committed = float(mo.summary.freeSpace or 0) / ByteFactors.KB_to_TB

        if hasattr(mo.summary, 'uncommitted'):
            self.total_used = ((self.total_capacity - self.total_free_committed) + (float(
                mo.summary.uncommitted or 0) / ByteFactors.KB_to_TB))
        else:
            self.total_used = (self.total_capacity - self.total_free_committed)

        # There may be files on the datastore that aren't in vcenter. Vcenter reports provisioned data
        # and these types of files are 'uncommitted' storage. So the actual free space and used space must
        # take into account the uncommitted space.
        self.total_free_actual = self.total_capacity - self.total_used
        self.vm_total = len(mo.vm)
        self.vm_poweredon_count = len([vm for vm in mo.vm if vm.runtime.powerState == 'poweredOn'])
        self.vm_template_count = len([vm for vm in mo.vm if vm.config.template])
        self.vm_poweredoff_count = self.vm_total - (self.vm_poweredon_count + self.vm_template_count)


class DatastoreClusterCapacity(CapacitySuper):

    def __init__(self, mo):
        self.name = mo.name
        self._mo_type = 'DatastoreCluster'
        self._mo_id = mo._moId
        self.total_capacity = float(mo.summary.capacity or 0) / ByteFactors.KB_to_TB
        # There may be files on the datastore that aren't in vcenter. Vcenter reports provisioned or 'committed'
        # data and these types of files are 'uncommitted' storage. So the actual free space and used space must
        # take into account the uncommitted space.
        self.total_free_committed = 0.0
        self.total_free_actual = 0.0
        self.total_used = 0.0
        self.datastores = None
        self.vm_total = 0
        self.vm_poweredon_count = 0
        self.vm_template_count = 0
        self.vm_poweredoff_count = 0

        self._get_storage_usage(mo)

    def _get_storage_usage(self, mo):
        self.datastores = [DatastoreCapacity(ds) for ds in mo.childEntity]
        for ds in self.datastores:
            self.total_free_committed += ds.total_free_committed
            self.total_free_actual += ds.total_free_actual
            self.total_used += ds.total_used
            self.vm_total += ds.vm_total
            self.vm_poweredon_count += ds.vm_poweredon_count
            self.vm_template_count += ds.vm_template_count
            self.vm_poweredoff_count += ds.vm_poweredoff_count
