#!/usr/bin/sh
# This script helps update the software in the machine image. 
# The command-line arguments are project, zone,
# the link where a zip archive of the project is located
# (In the case of Dropbox it is the link to the folder with ?dl=1),
# the name of the original image and of the new image.
# Should the two image names be same, the second image name may be ommitted.

project=$1
zone=$2
download_link=$3
orig_img=$4
new_img=${5:$orig_img}

name=temp-`date +%s`

# Create temp instance
gcloud beta compute instances create $name \
    --project=$project \
    --zone=$zone \
    --source-machine-image=projects/$project/global/machineImages/$orig_img \

# Update software

my_ssh()
{
    # Passing -x / to unzip to work with Dropbox-generated archives
    # https://stackoverflow.com/a/39448209/2725810
    gcloud compute ssh --zone $zone $name -- "rm -rf ExpoCloud && curl -L $download_link > ExpoCloud.zip && unzip ExpoCloud.zip -x / -d ExpoCloud && rm -f ExpoCloud.zip"
    result=$?
    echo $result
    return $result
}

until my_ssh; do
    echo "Waiting 5 seconds to try ssh again..."
    sleep 5
done

# Remove the old image
#gcloud compute images delete $client_img

# Create new image
gcloud beta compute machine-images create $new_img --project=$project --source-instance=$name --source-instance-zone=$zone

# Delete temp instance
gcloud compute instances delete --quiet --zone=$zone $name