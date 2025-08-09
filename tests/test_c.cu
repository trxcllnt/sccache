#include <stdio.h>

__device__ void cuda_device_func(int*, int*) {}
__global__ void cuda_entry_point(int* a, int* b) {
  cuda_device_func(a, b);
}

int main() {
  printf("%s says hello world\n", __FILE__);
}
