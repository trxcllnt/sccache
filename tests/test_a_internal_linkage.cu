#include <stdio.h>
#include "cuda_runtime.h"

namespace {  // ensure internal linkage of kernels
__device__ void cuda_device_func(int* a) {
  #  if __CUDA_ARCH__ < 860
  a[0] = 1;
  #  else
  a[0] = 2;
  #  endif
}

__global__ void cuda_entry_point(int* a) {
  cuda_device_func(a);
}
}

#ifdef __CUDA_ARCH__
static const auto cuda_arch_defined = true;
#else
static const auto cuda_arch_defined = false;
#endif

int main() {
  int* a;
  cudaMalloc(&a, sizeof(int));
  cuda_entry_point<<<1,1>>>(a);
  int b;
  cudaMemcpy(&b, a, sizeof(int), cudaMemcpyDeviceToHost);
  printf("%s says hello world, result=%d\n", __FILE__, b);
}
