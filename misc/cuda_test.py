import torch


def check_cuda():
    if torch.cuda.is_available():
        print("CUDA is available!")
        print(f"Number of CUDA devices: {torch.cuda.device_count()}")
        print(f"CUDA device name: {torch.cuda.get_device_name(0)}")
        print(f"CUDA version: {torch.version.cuda}")
    else:
        print("CUDA is not available.")


check_cuda()
