#
# Powershell script to build docker image:
#
$image = (Get-Content -Path ".\docker\_image-name.txt" -First 1)
#
# remove previous image:
#
docker image inspect $image *>$null
if ($LASTEXITCODE -eq 0) {
  docker rmi $image
}
#
# build new image:
#
docker build -t $image .\docker
