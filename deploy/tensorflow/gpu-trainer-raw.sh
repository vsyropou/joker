

# sourse: https://github.com/NVIDIA/nvidia-docker

# If you have nvidia-docker 1.0 installed: we need to remove it and all existing GPU containers
# docker volume ls -q -f driver=nvidia-docker | xargs -r -I{} -n1 docker ps -q -a -f volume={} | xargs -r docker rm -f
# sudo apt-get purge -y nvidia-docker

# Add the package repositories
curl -s -L https://nvidia.github.io/nvidia-docker/gpgkey | sudo apt-key add -
distribution=$(. /etc/os-release;echo $ID$VERSION_ID)
curl -s -L https://nvidia.github.io/nvidia-docker/$distribution/nvidia-docker.list | \
  sudo tee /etc/apt/sources.list.d/nvidia-docker.list
sudo apt-get update

# Install nvidia-docker2 and reload the Docker daemon configuration
sudo apt-get install -y nvidia-docker2
sudo pkill -SIGHUP dockerd


# Test nvidia-smi with the latest official CUDA image
docker run --runtime=nvidia --rm nvidia/cuda:9.0-base nvidia-smi

# Test tensorflow performance 
git clone https://github.com/tensorflow/models.git

mv  models/tutorials/image/cifar10 tensorflow-benchmark
rm -rf models

docker build -f ~/repos/tensorflow/tensorflow/tools/dockerfiles/dockerfiles/gpu.Dockerfile --build-arg TF_PACKAGE=tensorflow-gpu -t tfgpu .
docker run --runtime=nvidia -itd -v ~/repos/tensorflow-benchmark:/my-devel  -p 6006:6006 --name train-tfgpu tfgpu
docker exec -it train-tfgpu python /my-devel/cifar10_train.py
docker exec -it train-tfgpu tensorboard --logdir=/tmp


# prep python env
pip install conf/requirements-tf-gpu-trainer.txt
