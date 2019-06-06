
import math
import statistics
from vspherecapacity.capacity import CapacitySuper
from vspherecapacity.capacity.virtualmachine import VmSize
from vspherecapacity.capacity.byteconversion import ByteFactors


def avg(lst):
    return sum(lst) / len(lst)


class VsphereCapacity(CapacitySuper):

    # MB_FACTOR = 1048576
    # GB_FACTOR = 1073741824

    def __init__(self, vcenter_name, mo, hw_map=None, sql_map=None):
        self.vcenter = vcenter_name
        self.cluster = mo.parent.name
        self._parent_id = mo.parent._moId
        self.name = mo.name
        self._mo_id = mo._moId
        self.cpu_total = mo.hardware.cpuInfo.numCpuCores
        self.cpu_used = 0
        self.cpu_percent_used = 0.0
        self.cpu_over_commit = 0.0
        self.mem_total = float(mo.hardware.memorySize or 0)/ByteFactors.KB_to_GB
        self.mem_granted = float(mo.summary.quickStats.overallMemoryUsage or 0)/ByteFactors.MB_to_GB
        self.mem_granted_percent = 0.0
        self.mem_used = 0.0
        self.mem_over_commit = 0.0
        self.mem_percent_used = 0.0
        self.vm_count = 0
        self.uptime_days = float(float(mo.summary.quickStats.uptime or 0)/86400)
        self.virtualmachine = []
        self.vm_sizes = []
        self.vm_low_cpu = 0.0
        self.vm_low_mem = 0.0
        self.vm_avg_cpu = 0.0
        self.vm_avg_mem = 0.0
        self.vm_max_cpu = 0.0
        self.vm_max_mem = 0.0
        self.vendor = None
        self.model = None
        self.serial_number = None
        self.contract_expiry = None
        self.eol = None

        if not hw_map:
            hw_map = {}
        if not sql_map:
            sql_map = {}
        self._get_usage_info(mo, hw_map, sql_map)

    def _get_usage_info(self, mo, hw_map=None, sql_map=None):
        vm_size = []
        vm_cpu = []
        vm_mem = []
        short_name = mo.name.lower().replace('.nordstrom.net', '')
        if hw_map.get(short_name, None):
            # hw_map has data about this host
            self.serial_number = hw_map[short_name]['serial']
            self.model = hw_map[short_name]['model']
            if sql_map.get(short_name, None):
                dbo = sql_map.get(short_name, None)
                if dbo.common_name.lower() == short_name:
                    self.contract_expiry = dbo.contract_expiring
                    self.eol = dbo.end_of_life
        else:
            if sql_map.get(short_name, None):
                dbo = sql_map.get(short_name, None)
                if dbo.common_name.lower() == short_name:
                    self.serial_number = dbo.serial_number
                    self.contract_expiry = dbo.contract_expiring
                    self.eol = dbo.end_of_life
            self.model = mo.hardware.systemInfo.model

        self.vendor = mo.hardware.systemInfo.vendor
        for vm in mo.vm:
            # Todo VirtualMachineCapacity() .....
            if vm.runtime.powerState == 'poweredOn':
                vcpu = vm.config.hardware.numCPU
                vmem = float(vm.config.hardware.memoryMB or 0)/ByteFactors.MB_to_GB
                size_str = '{}x{}'.format(vcpu, vmem)
                if not size_str in vm_size:
                    vm_size.append(size_str)
                    self.vm_sizes.append(VmSize(size_str))
                vm_cpu.append(vcpu)
                vm_mem.append(vmem)
                self.cpu_used += vcpu
                self.mem_used += vmem
                self.vm_count += 1
        self.cpu_over_commit = self.cpu_used/self.cpu_total
        self.cpu_percent_used = self.cpu_over_commit*100
        self.mem_over_commit = self.mem_used/self.mem_total
        self.mem_percent_used = self.mem_over_commit*100
        self.mem_granted_percent = (self.mem_granted/self.mem_total)*100

        if not vm_cpu:
            vm_cpu = [0]
        if not vm_mem:
            vm_mem = [0]
        try:
            self.vm_low_cpu = min(vm_cpu)
            self.vm_low_mem = min(vm_mem)
            self.vm_avg_cpu = math.ceil(avg(vm_cpu))
            self.vm_avg_mem = math.ceil(avg(vm_mem))
            self.vm_max_cpu = max(vm_cpu)
            self.vm_max_mem = max(vm_mem)
        except ValueError as e:
            raise

    @staticmethod
    def map_indexes(self, vm_cpu, vm_mem):
        cpu_mem_map = {}
        mem_cpu_map = {}

        for i in range(len(vm_cpu)):
            if not cpu_mem_map.get(vm_cpu[i] or None):
                cpu_mem_map.update({vm_cpu[i]: []})
            if not mem_cpu_map.get(vm_mem[i] or None):
                mem_cpu_map.update({vm_mem[i]: []})
            cpu_mem_map[vm_cpu[i]].append(i)
            mem_cpu_map[vm_mem[i]].append(i)

        return [cpu_mem_map, mem_cpu_map]
