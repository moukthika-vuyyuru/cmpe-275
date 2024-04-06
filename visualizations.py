import sys
from multiprocessing import shared_memory, resource_tracker

shm_seg = shared_memory.SharedMemory(name='county_data_shared_memory')
data = bytes(shm_seg.buf).strip(b'\x00').decode('ascii')
shm_seg.close()

# Manually remove segment from resource_tracker, otherwise shmem segment
# will be unlinked upon program exit
resource_tracker.unregister(shm_seg._name, "shared_memory")

# remove the unwanted .5"}}} at the end of the string
data = data[:-6]

print(data)