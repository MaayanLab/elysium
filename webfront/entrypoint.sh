mkdir -p ~/.aws
echo "[default]\noutput=json\nregion=us-east-1" > ~/.aws/config
echo "[default]\naws_access_key_id=$AWSID\naws_secret_access_key=$AWSKEY" > ~/.aws/credentials

python mainalign.py