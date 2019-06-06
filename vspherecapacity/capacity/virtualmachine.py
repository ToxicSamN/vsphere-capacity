
from vspherecapacity.capacity import CapacitySuper


class VmSize(CapacitySuper):

    def __init__(self, vm_size):
        self.vm_size = vm_size
        self._mo_id = 'vmsize-{}'.format(vm_size)

    def __str__(self):
        return self.vm_size
